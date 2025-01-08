use crate::config::AppConfig;
use fhir_sdk::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_sdk::r4b::resources;
use fhir_sdk::r4b::resources::{
    BaseResource, Bundle, BundleEntry, BundleEntryRequest, DomainResource, IdentifiableResource,
    Medication, MedicationAdministration, MedicationAdministrationDosage,
    MedicationAdministrationEffective, MedicationAdministrationMedication, MedicationIngredient,
    NamedResource, Procedure, ProcedurePerformed, Resource, ResourceType,
};
use fhir_sdk::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity, Range, Reference};
use fhir_sdk::BuilderError;
use log::debug;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::iter::once;
use std::ops::Deref;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct Mapper {
    pub(crate) ops_mapping: HashMap<String, OpsMedication>,
    pub(crate) med_mapping: HashMap<String, OpsMedicationIngredient>,
}

impl Mapper {
    pub(crate) fn map(&self, bundle: String) -> Result<String, Box<dyn std::error::Error>> {
        // deserialize
        let b: Bundle = serde_json::from_str(bundle.as_str())?;

        // map Procedure to MedicationAdministration / Medication
        let ops_med: Vec<Option<BundleEntry>> = self
            .get_procedures(b)
            .iter()
            .map(|p| self.map_medication(p))
            .flatten()
            .flat_map(|res| once(bundle_entry(res.0)).chain(once(bundle_entry(res.1))))
            .map(Some)
            .collect();

        debug!("{:#?}", ops_med);
        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(ops_med)
            .build()?;

        // serialize
        let bundle = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(bundle)
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
    ) -> Result<(Medication, MedicationAdministration), Box<dyn std::error::Error>> {
        let Some(record): Option<&OpsMedication> =
            self.get_med_mapping(procedure.code.clone().ok_or("procedure code missing")?)
        else {
            return Err("No code found for procedure".into());
        };

        // build medication
        let medication_id = build_hash(record.substanzangabe_aus_ops_code.to_owned());
        let medication = Medication::builder()
            .id(medication_id.clone())
            .meta(
                build_meta("https://www.medizininformatik-initiative.de/fhir/core/modul-medikation/StructureDefinition/Medication",
                           procedure))
            .identifier(vec![Some(Identifier::builder()
                .system("https://fhir.diz.uni-marburg.de/sid/medication-id".to_owned())
                .value(medication_id.clone()).build().unwrap())])
            .code(
                CodeableConcept::builder()
                    .coding(vec![Some(
                        Coding::builder()
                            .system("http://fhir.de/CodeSystem/bfarm/atc".to_owned())
                            .code(record.atc_code.clone())
                            .version("ATC/DDD Version 2020".to_owned())
                            .build()
                            .unwrap(),
                    )])
                    .build()
                    .unwrap(),
            )
            .status("active".to_owned())
            // TODO set ingredient
            .ingredient(self.build_ingredient(record.atc_code.clone()))
            .build()?;

        // create MedicationAdministration
        let med_admin = build_medication_administration(
            procedure,
            record,
            medication.id.clone().expect("medication missing id"),
        )?;

        Ok((medication, med_admin))
    }

    fn get_med_mapping(&self, codes: CodeableConcept) -> Option<&OpsMedication> {
        codes
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

    fn build_ingredient(&self, atc_code: String) -> Vec<Option<MedicationIngredient>> {
        // TODO
        // medication.setIngredient(List.of(new MedicationIngredientComponent().setItem(new CodeableConcept()
        //     .addCoding(new Coding()
        //         .setCode(medikament.getSubstanzGenauUniiNumber())
        //         .setSystem("http://fdasis.nlm.nih.gov"))
        //     .addCoding(new Coding()
        //         .setCode(medikament.getSubstanzGenauAskNr())
        //         .setSystem("http://fhir.de/CodeSystem/ask"))
        //     .addCoding(new Coding()
        //         .setCode(medikament.getSubstanzGenauCasNummer())
        //         .setSystem("urn:oid:2.16.840.1.113883.6.61")))));

        todo!()
        // self.med_mapping.
    }
}

fn build_hash(input: String) -> String {
    let mut s = DefaultHasher::new();
    input.hash(&mut s);
    s.finish().to_string()
}

fn bundle_entry<T: IdentifiableResource + Clone>(resource: T) -> BundleEntry
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
        .expect("missing identifier with use: 'usual'");

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
        .expect("failed to build Bundle entry")
}

fn build_medication_administration(
    procedure: &Procedure,
    record: &OpsMedication,
    medication_id: String,
) -> Result<MedicationAdministration, Box<dyn Error>> {
    let procedure_id = procedure.id.clone().expect("missing procedure id");
    MedicationAdministration::builder()
        // set required properties
        .id(procedure_id.clone())
        .meta(
            build_meta("https://www.medizininformatik-invecitiative.de/fhir/core/modul-medikation/StructureDefinition/MedicationAdministration",
                       procedure))
        .identifier(vec![Some(Identifier::builder()
            .system("https://fhir.diz.uni-marburg.de/sid/medication-administration-id".to_owned())
            .value(procedure_id.clone()).build().unwrap())])
        .status(procedure.status.to_string())
        .subject(procedure.subject.clone())
        .part_of(vec![Some(procedure_ref(procedure.identifier.clone()).ok_or("failed to build procedure reference from identifier")?)])
        .effective(build_effective(procedure.performed.clone().ok_or("procedure.perfomed is empty")?)?)
        .dosage(build_dosage(record)?)
        .medication(MedicationAdministrationMedication::Reference(Reference::builder().reference(
            conditional_reference(
                ResourceType::Medication,"https://diz.uni-marburg.de/fhir/sid/medication-administration-id".to_owned(),
                medication_id)
        ).build()?))
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
    identifiers
        .iter()
        .flatten()
        .filter_map(|i| {
            // use USUAL identifier for now
            if i.r#use? == IdentifierUse::Usual {
                Some(
                    Reference::builder()
                        .reference(format!(
                            "identifier={}|{}",
                            i.system.clone()?,
                            i.value.clone()?
                        ))
                        .build()
                        .unwrap(),
                )
            } else {
                None
            }
        })
        .next()
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
    pub(crate) fn new(config: AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // get resource dir
        let mut base_dir = env::current_dir()?.join("resources");
        // base_dir.push("resources");
        // base_dir.push("alleOPS_ab2008_Mapping.csv");

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
    ) -> Result<HashMap<String, T>, Box<dyn std::error::Error>> {
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
