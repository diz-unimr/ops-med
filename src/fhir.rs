use crate::config::AppConfig;
use fhir_sdk::r4b::codes::IdentifierUse;
use fhir_sdk::r4b::resources::{
    Bundle, Medication, MedicationAdministration, MedicationAdministrationDosage,
    MedicationAdministrationEffective, Procedure, ProcedurePerformed,
};
use fhir_sdk::r4b::types::{
    CodeableConcept, Coding, Identifier, IdentifierBuilder, Meta, Quantity, Range, Reference,
    ReferenceBuilder,
};
use fhir_sdk::BuilderError;
use futures::StreamExt;
use log::debug;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone)]
pub(crate) struct Mapper {
    pub(crate) med_mapping: HashMap<String, Record>,
}

impl Mapper {
    pub(crate) fn map(&self, bundle: String) -> Result<String, Box<dyn std::error::Error>> {
        // deserialize
        let b: Bundle = serde_json::from_str(bundle.as_str())?;

        // map Procedure to MedicationAdministration / Medication
        let ops_med: Vec<Medication> = self
            .get_procedures(b)
            .iter()
            .map(|p| self.map_medication(p))
            .flatten()
            .collect();

        debug!("{:#?}", ops_med);

        // TODO serialize

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
    ) -> Result<Medication, Box<dyn std::error::Error>> {
        let Some(record): Option<&Record> =
            self.get_med_mapping(procedure.code.clone().ok_or("procedure code missing")?)
        else {
            return Err("No code found for procedure".into());
        };

        // create MedicationAdministration
        let _ = create_medication_administration(procedure, record);

        // TODO
        let med_builder = Medication::builder().code(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://fhir.de/CodeSystem/bfarm/atc".to_owned())
                        .code(record.clone().atc_code)
                        .version("ATC/DDD Version 2020".to_owned())
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        med_builder.build().map_err(|e| e.into())
    }

    fn get_med_mapping(&self, codes: CodeableConcept) -> Option<&Record> {
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
            .filter_map(|ops_code| self.med_mapping.get::<str>(ops_code.as_ref()))
            .next()
    }
}

fn create_medication_administration(
    procedure: &Procedure,
    record: &Record,
) -> Result<MedicationAdministration, Box<dyn Error>> {
    MedicationAdministration::builder()
        // set required properties
        .meta(
            build_meta("https://www.medizininformatik-invecitiative.de/fhir/core/modul-medikation/StructureDefinition/MedicationAdministration",
                       procedure))
        .identifier(vec![Some(Identifier::builder()
            .system("foo".to_string())
            .value("bar".to_string()).build().unwrap())])
        .status(procedure.status.to_string())
        .subject(procedure.subject.clone())
        .part_of(vec![Some(procedure_ref(procedure.identifier.clone()).ok_or_else(||"failed to build procedure reference from identifier")?)])
        .effective(build_effective(procedure.performed.clone().ok_or("procedure.perfomed is empty")?)?)
        .dosage(build_dosage(record)?)
        .build()
        // optional properties
        .map(|mut admin| {
            // set encounter
            admin.context = procedure.encounter.clone();
            admin
        })
        .map_err(|e| e.into())
}

fn build_dosage(record: &Record) -> Result<MedicationAdministrationDosage, BuilderError> {
    MedicationAdministrationDosage::builder()
        .text(record.text.clone())
        // TODO SimpleQuantity instead of Range in Administration vs Statement
        // .dose(build_dose(record)?)
        // TODO SNOMED mapping
        // .site(...)
        .route(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://standardterms.edqm.eu".to_string())
                        .code(record.route_code.to_string())
                        .display(record.route_name.to_string())
                        .build()?,
                )])
                .build()?,
        )
        .method(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://standardterms.edqm.eu".to_string())
                        .code(record.method_code.to_string())
                        .display(record.method_name.to_string())
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
            // TODO configure which identifier to use
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
    let builder = Meta::builder().profile(vec![Some(profile.to_string())]);
    let builder = match procedure.meta.as_ref().and_then(|m| m.source.clone()) {
        Some(source) => builder.source(source),
        None => builder,
    };

    builder.build().unwrap()
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Record {
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
    // substanz_genau_unii_number: String,
    // substanz_genau_ask_nr: String,
    // substanz_genau_cas_nummer: String,
}

impl Mapper {
    pub(crate) fn new(config: AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // get resource dir
        let mut path = env::current_dir()?;
        path.push("resources");
        path.push("alleOPS_ab2008_Mapping.csv");

        let ops_mapping = Mapper::read_csv(path)?;

        Ok(Mapper {
            med_mapping: ops_mapping,
        })
    }

    pub(crate) fn read_csv<P: AsRef<Path>>(
        filename: P,
    ) -> Result<HashMap<String, Record>, Box<dyn std::error::Error>> {
        let file = File::open(filename)?;
        let mut reader = csv::ReaderBuilder::new().delimiter(b';').from_reader(file);
        let mut map = HashMap::new();

        for result in reader.deserialize() {
            let record: Record = result?;
            map.insert(record.ops_code.to_string(), record);
            // Try this if you don't like each record smushed on one line:
            // println!("{:#?}", record);
        }

        Ok(map)
    }
}
