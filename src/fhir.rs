use crate::config::{AppConfig, Fhir};
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Medication,
    MedicationIngredient, MedicationIngredientItem, MedicationStatement,
    MedicationStatementEffective, MedicationStatementMedication, Procedure, ProcedurePerformed,
    Resource, ResourceType,
};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Dosage, DosageDoseAndRate, DosageDoseAndRateDose, Identifier, Meta,
    Quantity, Range, Reference,
};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct Mapper {
    pub(crate) config: Fhir,
    pub(crate) ops_mapping: HashMap<String, OpsMedication>,
    pub(crate) med_mapping: HashMap<String, OpsMedicationIngredient>,
}

impl Mapper {
    pub(crate) fn map(&self, bundle: String) -> Result<Option<String>, Box<dyn Error>> {
        // deserialize
        let b: Bundle = serde_json::from_str(bundle.as_str())?;

        // map Procedure to MedicationStatement / Medication
        let ops_med: Vec<Option<BundleEntry>> = self
            .get_procedures(b)
            .iter()
            .filter_map(|p| self.get_med_mapping(p.code.clone()).map(|m| (p, m)))
            .map(|(p, m)| self.map_medication(p, m))
            .map(|res| to_bundle_entry(res))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        if ops_med.is_empty() {
            return Ok(None);
        }

        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(ops_med)
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn get_procedures(&self, bundle: Bundle) -> Vec<Procedure> {
        bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|entry| entry.resource.as_ref())
            .filter_map(|resource| <&Procedure>::try_from(resource).ok())
            .cloned()
            .collect()
    }

    fn build_medication_statement(
        &self,
        procedure: &Procedure,
        record: &OpsMedication,
        medication_id: String,
    ) -> Result<MedicationStatement, Box<dyn Error>> {
        let procedure_id = default_identifier(procedure.identifier.clone())
            .ok_or("no default identifier")?
            .value
            .clone()
            .ok_or("missing value for default identifier")?;
        MedicationStatement::builder()
            // set required properties
            .id(procedure_id.clone())
            .meta(build_meta(
                self.config.medication_statement.profile.to_owned(),
                procedure,
            ))
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system(self.config.medication_statement.system.to_owned())
                    .value(procedure_id.clone())
                    .build()
                    .unwrap(),
            )])
            .status(procedure.status.to_string())
            .subject(procedure.subject.clone())
            .part_of(vec![Some(
                procedure_ref(procedure.identifier.clone())
                    .ok_or("failed to build procedure reference from identifier")?,
            )])
            .effective(build_effective(
                procedure
                    .performed
                    .clone()
                    .ok_or("procedure.perfomed is empty")?,
            )?)
            .dosage(vec![Some(build_dosage(record)?)])
            .medication(MedicationStatementMedication::Reference(
                Reference::builder()
                    .reference(conditional_reference(
                        ResourceType::Medication,
                        self.config.medication.system.to_owned(),
                        medication_id,
                    ))
                    .display(record.substanzangabe_aus_ops_code.to_owned())
                    .build()?,
            ))
            .build()
            .map(|mut admin| {
                // need to be able to set Option for context (encounter)
                admin.context = procedure.encounter.clone();
                admin
            })
            .map_err(|e| e.into())
    }

    fn map_medication(
        &self,
        procedure: &Procedure,
        ops_med: &OpsMedication,
    ) -> Result<(Medication, MedicationStatement), Box<dyn Error>> {
        // build medication
        let medication_id = build_legacy_id(ops_med.substanzangabe_aus_ops_code.to_owned());
        let medication = Medication::builder()
            .id(medication_id.clone())
            .meta(build_meta(
                self.config.medication.profile.to_owned(),
                procedure,
            ))
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system(self.config.medication.system.to_owned())
                    .value(medication_id.clone())
                    .build()
                    .unwrap(),
            )])
            .code(
                CodeableConcept::builder()
                    .coding(vec![Some(
                        Coding::builder()
                            .system("http://fhir.de/CodeSystem/bfarm/atc".to_owned())
                            .code(ops_med.atc_code.clone())
                            .version("ATC/DDD Version 2020".to_owned())
                            .build()
                            .unwrap(),
                    )])
                    .build()
                    .unwrap(),
            )
            .status("active".to_owned())
            .ingredient(
                self.build_ingredient(
                    self.med_mapping
                        .get::<str>(ops_med.atc_code.as_ref())
                        .ok_or("failed to find mapping for Medication.ingredient".to_owned())?,
                ),
            )
            .build()?;

        // MedicationStatement
        let med_statement = self.build_medication_statement(
            procedure,
            ops_med,
            medication.id.clone().expect("medication missing id"),
        )?;

        Ok((medication, med_statement))
    }

    fn get_med_mapping(&self, codes: Option<CodeableConcept>) -> Option<&OpsMedication> {
        codes?
            .coding
            .iter()
            .flatten()
            .filter_map(|c| {
                if c.system.clone()? == "http://fhir.de/CodeSystem/bfarm/ops" {
                    c.code.clone()
                } else {
                    None
                }
            })
            .filter_map(|ops_code| self.ops_mapping.get::<str>(ops_code.as_ref()))
            .filter(|&m| self.med_mapping.contains_key::<str>(m.atc_code.as_ref()))
            .next()
    }

    fn build_ingredient(
        &self,
        mapping: &OpsMedicationIngredient,
    ) -> Vec<Option<MedicationIngredient>> {
        let ingredient = self.med_mapping.get::<str>(mapping.atc_code.as_ref());
        // TODO optional?
        if let Some(&ref ingredient) = ingredient {
            vec![Some(
                MedicationIngredient::builder()
                    .item(MedicationIngredientItem::CodeableConcept(
                        CodeableConcept::builder()
                            .coding(vec![
                                Some(
                                    Coding::builder()
                                        .system("http://fhir.de/CodeSystem/ask".to_owned())
                                        .code(ingredient.substanz_genau_ask_nr.to_owned())
                                        .build()
                                        .unwrap(),
                                ),
                                Some(
                                    Coding::builder()
                                        .system("http://fdasis.nlm.nih.gov".to_owned())
                                        .code(ingredient.substanz_genau_unii_number.to_owned())
                                        .build()
                                        .unwrap(),
                                ),
                                Some(
                                    Coding::builder()
                                        .system("urn:oid:2.16.840.1.113883.6.61".to_owned())
                                        .code(ingredient.substanz_genau_cas_nummer.to_owned())
                                        .build()
                                        .unwrap(),
                                ),
                            ])
                            .build()
                            .unwrap(),
                    ))
                    .build()
                    .unwrap(),
            )]
        } else {
            vec![]
        }
    }
}

fn to_bundle_entry(
    medication: Result<(Medication, MedicationStatement), Box<dyn Error>>,
) -> Result<Vec<Option<BundleEntry>>, Box<dyn Error>> {
    match medication {
        Ok(med) => Ok(vec![
            Some(bundle_entry(med.0.clone())?),
            Some(bundle_entry(med.1.clone())?),
        ]),
        Err(e) => Err(e),
    }
}

fn build_legacy_id(input: String) -> String {
    let hash = farmhash::fingerprint64(input.as_bytes());
    // convert to big endian and hex
    format!("id-{:x}", hash.to_be())
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
) -> Result<BundleEntry, Box<dyn Error>>
where
    Resource: From<T>,
{
    // resource type
    let resource_type = Resource::from(resource.clone()).resource_type();

    // identifier
    let identifier = resource
        .identifier()
        .iter()
        .flatten()
        .filter(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .next()
        .ok_or("missing identifier with use: 'usual'")?;

    BundleEntry::builder()
        .resource(resource.clone().into())
        .request(
            BundleEntryRequest::builder()
                .method(HTTPVerb::Put)
                .url(conditional_reference(
                    resource_type,
                    identifier
                        .system
                        .clone()
                        .expect("identifier.system missing")
                        .to_owned(),
                    identifier
                        .value
                        .clone()
                        .expect("identifier.value missing")
                        .to_owned(),
                ))
                .build()
                .expect("failed to build BundeEntryRequest"),
        )
        .build()
        .map_err(|e| e.into())
        .into()
}

fn conditional_reference(resource_type: ResourceType, system: String, value: String) -> String {
    format!("{resource_type}?identifier={system}|{value}")
}

fn build_dosage(record: &OpsMedication) -> Result<Dosage, Box<dyn Error>> {
    Dosage::builder()
        .text(record.text.clone())
        .dose_and_rate(vec![Some(build_dose(record)?)])
        // TODO needs SNOMED mapping
        // .site(...)
        .route(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://standardterms.edqm.eu".to_owned())
                        .code(record.route_code.to_owned())
                        .display(record.route_name.to_owned())
                        .build()?,
                )])
                .build()?,
        )
        .method(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://standardterms.edqm.eu".to_owned())
                        .code(record.method_code.to_owned())
                        .display(record.method_name.to_owned())
                        .build()?,
                )])
                .build()?,
        )
        .build()
        .map_err(|e| e.into())
}

fn build_dose(ops_med: &OpsMedication) -> Result<DosageDoseAndRate, Box<dyn Error>> {
    let mut range = Range::builder().build()?;

    if let Some(unit_low) = &ops_med.einheit_min {
        range.low = Some(ucum_quantity(unit_low, ops_med)?);
    }
    if let Some(unit_high) = &ops_med.einheit_max {
        range.high = Some(ucum_quantity(unit_high, ops_med)?);
    }

    DosageDoseAndRate::builder()
        .dose(DosageDoseAndRateDose::Range(range))
        .r#type(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://terminology.hl7.org/CodeSystem/dose-rate-type".to_owned())
                        .code("calculated".to_owned())
                        .display("calculated".to_owned())
                        .build()?,
                )])
                .build()?,
        )
        .build()
        .map_err(|e| e.into())
}

fn ucum_quantity(value: &String, med: &OpsMedication) -> Result<Quantity, Box<dyn Error>> {
    Quantity::builder()
        .value(value.trim().replace(',', ".").parse::<f64>()?)
        .system("http://unitsofmeasure.org".to_owned())
        .unit(med.ucum_description.to_owned())
        .code(med.ucum_code.to_owned())
        .build()
        .map_err(|e| e.into())
}

fn build_effective(
    performed: ProcedurePerformed,
) -> Result<MedicationStatementEffective, Box<dyn Error>> {
    match performed {
        ProcedurePerformed::DateTime(dt) => Ok(MedicationStatementEffective::DateTime(dt)),
        ProcedurePerformed::Period(p) => Ok(MedicationStatementEffective::Period(p)),
        _ => Err("Procedure.performed must be DateTime or Period".into()),
    }
}

fn procedure_ref(identifiers: Vec<Option<Identifier>>) -> Option<Reference> {
    default_identifier(identifiers).map(|id| {
        Reference::builder()
            .reference(format!(
                "Procedure?identifier={}|{}",
                id.system.clone().unwrap(),
                id.value.clone().unwrap()
            ))
            .build()
            .unwrap()
    })
}

fn default_identifier(identifiers: Vec<Option<Identifier>>) -> Option<Identifier> {
    match identifiers.iter().flatten().count() == 1 {
        true => identifiers.into_iter().next().unwrap(),
        false => identifiers
            .iter()
            .flatten()
            .filter_map(|i| {
                // use USUAL identifier for now
                if i.r#use? == IdentifierUse::Usual {
                    Some(i.clone())
                } else {
                    None
                }
            })
            .next(),
    }
}

fn build_meta(profile: String, procedure: &Procedure) -> Meta {
    let builder = Meta::builder().profile(vec![Some(profile)]);
    let builder = match procedure.meta.as_ref().and_then(|m| m.source.clone()) {
        Some(source) => builder.source(source),
        None => builder,
    };

    builder.build().unwrap()
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OpsMedication {
    #[serde(rename = "OPS-code")]
    ops_code: String,
    #[serde(rename = "Text")]
    text: String,
    #[serde(rename = "Substanzangabe_aus_OPS-Code")]
    substanzangabe_aus_ops_code: String,
    #[serde(rename = "ATC-Code")]
    atc_code: String,
    #[serde(rename = "Routes and Methods of Administration - Concept Code")]
    route_code: String,
    #[serde(rename = "Routes and Methods of Administration - Term")]
    route_name: String,
    #[serde(rename = "Administration Method - ID")]
    method_code: String,
    #[serde(rename = "Administration Method - Name")]
    method_name: String,
    #[serde(rename = "Einheit_Wert_min")]
    einheit_min: Option<String>,
    #[serde(rename = "Einheit_Wert_max")]
    einheit_max: Option<String>,
    #[serde(rename = "UCUM-Description")]
    ucum_description: String,
    #[serde(rename = "UCUM-Code")]
    ucum_code: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OpsMedicationIngredient {
    #[serde(rename = "ATC-Code")]
    atc_code: String,
    #[serde(rename = "Substanz_genau_UNII-number")]
    substanz_genau_unii_number: String,
    #[serde(rename = "Substanz_genau_ASK-Nr")]
    substanz_genau_ask_nr: String,
    #[serde(rename = "Substanz_genau_CAS-Nummer")]
    substanz_genau_cas_nummer: String,
}

impl Mapper {
    pub(crate) fn new(config: AppConfig) -> Result<Self, Box<dyn Error>> {
        // get resource dir
        let base_dir = env::current_dir()?.join("resources");

        let ops_med_path = base_dir.join("alleOPS_ab2008_Mapping.csv");
        let med_path = base_dir.join("alleSubstanzenMapping.csv");

        let ops_mapping = Mapper::read_csv::<OpsMedication>(ops_med_path, |o| o.ops_code)?;
        let med_mapping = Mapper::read_csv::<OpsMedicationIngredient>(med_path, |m| m.atc_code)?;

        Ok(Mapper {
            config: config.fhir,
            ops_mapping,
            med_mapping,
        })
    }

    pub(crate) fn read_csv<T: DeserializeOwned + Clone>(
        filename: PathBuf,
        selector: fn(T) -> String,
    ) -> Result<HashMap<String, T>, Box<dyn Error>> {
        let file = File::open(filename)?;
        let mut reader = csv::ReaderBuilder::new().delimiter(b';').from_reader(file);
        let mut map = HashMap::new();

        for result in reader.deserialize() {
            let record: T = result?;
            map.insert(selector(record.clone()).to_owned(), record);
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use crate::fhir::build_legacy_id;

    #[test]
    fn test_hash() {
        let hash = build_legacy_id("Filgrastim".to_string());
        assert_eq!(hash, "id-23eebc8a034c196d");
    }
}
