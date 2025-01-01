use crate::config::AppConfig;
use fhir_sdk::r4b::resources::{Bundle, Medication, MedicationAdministration, Procedure};
use fhir_sdk::r4b::types::{CodeableConcept, Coding, Identifier, IdentifierBuilder, Meta};
use log::debug;

use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::Path;

#[derive(Debug, Clone)]
pub(crate) struct Mapper {
    pub(crate) med_mapping: HashMap<String, Record>,
}

impl Mapper {
    pub(crate) fn map(&self, bundle: String) -> Result<String, Box<dyn std::error::Error>> {
        // deserialize
        let b: Bundle = serde_json::from_str(bundle.as_str())?;

        // get procedures
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
        // .next()

        // .clone()
        // .ok_or("procedure code missing")?
        // .coding
        // .iter()
        // .flatten()
        // .filter_map(get_med_mapping)
        // .next()
        else {
            return Err("No code found for procedure".into());
        };

        // create MedicationAdministration
        create_medication_administration(procedure, record);

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
        .meta(
            build_meta("https://www.medizininformatik-invecitiative.de/fhir/core/modul-medikation/StructureDefinition/MedicationAdministration",
                         procedure))
        // TODO
        .identifier(vec![Some(Identifier::builder()
            .system("foo".to_string())
            .value("bar".to_string()).build().unwrap())])
        .status(procedure.status.to_string())
        .subject(procedure.subject.clone())
        // .context(procedure.encounter.unwrap())
        .build()
        .map_err(|e| e.into())
    // med_admin.meta.unwrap().source=procedure.meta.as_ref().and_then(|m|m.source.clone())

    // if procedure.meta.as_ref().and_then(|m|m.source.clone()))
    // med_admin
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
    #[serde(rename = "Substanzangabe_aus_OPS-Code")]
    substanzangabe_aus_ops_code: String,
    #[serde(rename = "ATC-Code")]
    atc_code: String,
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
