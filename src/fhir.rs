use crate::config::AppConfig;
use fhir_sdk::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_sdk::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Medication,
    MedicationAdministration, MedicationAdministrationDosage, MedicationAdministrationEffective,
    MedicationAdministrationMedication, MedicationIngredient, MedicationIngredientItem, Procedure,
    ProcedurePerformed, Resource, ResourceType,
};
use fhir_sdk::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Reference};
use fhir_sdk::BuilderError;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(crate) struct Mapper {
    pub(crate) ops_mapping: HashMap<String, OpsMedication>,
    pub(crate) med_mapping: HashMap<String, OpsMedicationIngredient>,
}

impl Mapper {
    pub(crate) fn map(&self, bundle: String) -> Result<Option<String>, Box<dyn Error>> {
        // deserialize
        let b: Bundle = serde_json::from_str(bundle.as_str())?;

        // map Procedure to MedicationAdministration / Medication
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

    fn map_medication(
        &self,
        procedure: &Procedure,
        ops_med: &OpsMedication,
    ) -> Result<(Medication, MedicationAdministration), Box<dyn Error>> {
        // build medication
        let medication_id = build_hash(ops_med.substanzangabe_aus_ops_code.to_owned());
        let medication = Medication::builder()
            .id(medication_id.clone())
            .meta(
                build_meta("https://www.medizininformatik-initiative.de/fhir/core/modul-medikation/StructureDefinition/Medication",
                           procedure))
            .identifier(vec![Some(Identifier::builder()
                .r#use(IdentifierUse::Usual)
                .system("https://fhir.diz.uni-marburg.de/sid/medication-id".to_owned())
                .value(medication_id.clone()).build().unwrap())])
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
            .ingredient(self.build_ingredient(self.med_mapping.get::<str>(ops_med.atc_code.as_ref())
                .ok_or("failed to find mapping for Medication.ingredient".to_owned())?))
            .build()?;

        // create MedicationAdministration
        let med_admin = build_medication_administration(
            procedure,
            ops_med,
            medication.id.clone().expect("medication missing id"),
        )?;

        Ok((medication, med_admin))
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
    medication: Result<(Medication, MedicationAdministration), Box<dyn Error>>,
) -> Result<Vec<Option<BundleEntry>>, Box<dyn Error>> {
    match medication {
        Ok(med) => Ok(vec![
            Some(bundle_entry(med.0.clone())?),
            Some(bundle_entry(med.1.clone())?),
        ]),
        Err(e) => Err(e),
    }
}

fn build_hash(input: String) -> String {
    let mut s = DefaultHasher::new();
    input.hash(&mut s);
    s.finish().to_string()
}

fn bundle_entry<T: IdentifiableResource + Clone>(resource: T) -> Result<BundleEntry, Box<dyn Error>>
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

fn build_medication_administration(
    procedure: &Procedure,
    record: &OpsMedication,
    medication_id: String,
) -> Result<MedicationAdministration, Box<dyn Error>> {
    let procedure_id = default_identifier(procedure.identifier.clone())
        .ok_or("no default identifier")?
        .value
        .clone()
        .ok_or("missing value for default identifier")?;
    MedicationAdministration::builder()
        // set required properties
        .id(procedure_id.clone())
        .meta(
            build_meta("https://www.medizininformatik-initiative.de/fhir/core/modul-medikation/StructureDefinition/MedicationAdministration",
                       procedure))
        .identifier(vec![Some(Identifier::builder()
            .r#use(IdentifierUse::Usual)
            .system("https://fhir.diz.uni-marburg.de/sid/medication-administration-id".to_owned())
            .value(procedure_id.clone()).build().unwrap())])
        .status(procedure.status.to_string())
        .subject(procedure.subject.clone())
        .part_of(vec![Some(procedure_ref(procedure.identifier.clone()).ok_or("failed to build procedure reference from identifier")?)])
        .effective(build_effective(procedure.performed.clone().ok_or("procedure.perfomed is empty")?)?)
        .dosage(build_dosage(record)?)
        .medication(MedicationAdministrationMedication::Reference(Reference::builder()
            .reference(conditional_reference(
                ResourceType::Medication,"https://diz.uni-marburg.de/fhir/sid/medication-administration-id".to_owned(),
                medication_id))
            .display(record.substanzangabe_aus_ops_code.to_owned()).build()?))
        .build()
        .map(|mut admin| {
            // need to be able to set Option for context (encounter)
            admin.context = procedure.encounter.clone();
            admin
        })
        .map_err(|e| e.into())
}

fn conditional_reference(resource_type: ResourceType, system: String, value: String) -> String {
    format!("{resource_type}?identifier={system}|{value}")
}

fn build_dosage(record: &OpsMedication) -> Result<MedicationAdministrationDosage, BuilderError> {
    MedicationAdministrationDosage::builder()
        .text(record.text.clone())
        // TODO exact dose is unknown as OPS codes represent a range
        // .dose(build_dose(record)?)
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
}

fn build_effective(
    performed: ProcedurePerformed,
) -> Result<MedicationAdministrationEffective, Box<dyn Error>> {
    match performed {
        ProcedurePerformed::DateTime(dt) => Ok(MedicationAdministrationEffective::DateTime(dt)),
        ProcedurePerformed::Period(p) => Ok(MedicationAdministrationEffective::Period(p)),
        _ => Err("Procedure.performed must be DateTime or Period".into()),
    }
}

fn procedure_ref(identifiers: Vec<Option<Identifier>>) -> Option<Reference> {
    default_identifier(identifiers).map(|id| {
        Reference::builder()
            .reference(format!(
                "identifier={}|{}",
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

fn build_meta(profile: &str, procedure: &Procedure) -> Meta {
    let builder = Meta::builder().profile(vec![Some(profile.to_owned())]);
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
