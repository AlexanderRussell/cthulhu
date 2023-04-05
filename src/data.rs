#![allow(dead_code)]
use crate::eq_floaties::{F32, F64};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Display;
use velvet::hashing::blake_3;
// use evmap_derive::ShallowCopy;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, PartialOrd, Eq, Hash, Ord)]
/// An enum to contain the different types of data that can be stored in a `Row`.
/// Directly corelates to the `DataType` enum.
pub enum Data {
    Bytes(Vec<u8>),
    String(String),
    Bool(bool),
    UInt(usize),
    IInt(isize),
    Float64(F64),
    Float32(F32),
    Null,
}

impl Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::String(string) => write!(f, "{}", string),
            Data::Bytes(bytes) => write!(f, "{:?}", bytes),
            Data::Bool(bool) => write!(f, "{}", bool),
            Data::UInt(u) => write!(f, "{}", u),
            Data::Float32(f32) => write!(f, "{}", f32),
            Data::Float64(f64) => write!(f, "{}", f64),
            Data::IInt(i) => write!(f, "{}", i),
            Data::Null => write!(f, "Null"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
/// An enum to represent the different types of data that can be stored in a `Row`.
/// Directly corelates to the `Data` enum.
pub enum DataType {
    Bytes,
    String,
    UInt,
    IInt,
    F64,
    Bool,
    F32,
    Null,
}

impl Data {
    pub fn update_inner<D>(&mut self, data: D)
    where
        D: AsData,
    {
        *self = data.as_data();
    }

    pub fn as_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    pub fn from_json(string: &str) -> Result<Self, Box<dyn Error>> {
        Ok(serde_json::from_str(string)?)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Data::Bytes(_) => DataType::Bytes,
            Data::String(_) => DataType::String,
            Data::UInt(_) => DataType::UInt,
            Data::Float64(_) => DataType::F64,
            Data::Float32(_) => DataType::F32,
            Data::Bool(_) => DataType::Bool,
            Data::IInt(_) => DataType::UInt,
            Data::Null => DataType::Null,
        }
    }

    pub fn get_string(&self) -> Option<String> {
        match self {
            Data::String(string) => Some(string.clone()),
            _ => None,
        }
    }

    pub fn get_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Data::Bytes(bytes) => Some(bytes.clone()),
            _ => None,
        }
    }
}

pub trait StrToData {
    fn str_to_data(&self) -> Data;
}

pub trait StrToDataType {
    fn str_to_data_type(&self) -> DataType;
}

impl StrToData for String {
    fn str_to_data(&self) -> Data {
        if let Ok(a_bool) = self.parse::<bool>() {
            Data::Bool(a_bool)
        } else if let Ok(a_usize) = self.parse::<usize>() {
            Data::UInt(a_usize)
        } else if let Ok(a_f64) = self.parse::<f64>() {
            Data::Float64(a_f64.into())
        } else if let Ok(a_f32) = self.parse::<f32>() {
            Data::Float32(a_f32.into())
        } else if let Ok(a_bytes) = serde_json::from_str::<Vec<u8>>(&self) {
            Data::Bytes(a_bytes)
        } else {
            Data::String(self.to_string())
        }
    }
}

impl StrToDataType for String {
    /// This function is used to determine the type of `Data` that a `String` represents.
    /// The primary use is for importing data from a csv or xlsx file into a `Table`.
    fn str_to_data_type(&self) -> DataType {
        if let Ok(_) = self.parse::<bool>() {
            DataType::Bool
        } else if let Ok(_) = self.parse::<usize>() {
            DataType::UInt
        } else if let Ok(_) = self.parse::<f64>() {
            DataType::F64
        } else if let Ok(_) = self.parse::<f32>() {
            DataType::F32
        } else if let Ok(_) = serde_json::from_str::<Vec<u8>>(&self) {
            DataType::Bytes
        } else {
            DataType::String
        }
    }
}

pub trait FromData<D>
where
    D: AsData,
{
    fn from_data(&self) -> D;
}

impl FromData<bool> for Data {
    fn from_data(&self) -> bool {
        match self {
            Data::Bool(a_bool) => *a_bool,
            _ => panic!("Cannot convert {:?} to bool", self),
        }
    }
}

impl FromData<usize> for Data {
    fn from_data(&self) -> usize {
        match self {
            Data::UInt(a_usize) => *a_usize,
            _ => panic!("Cannot convert {:?} to usize", self),
        }
    }
}

impl FromData<F64> for Data {
    fn from_data(&self) -> F64 {
        match self {
            Data::Float64(a_f64) => *a_f64,
            _ => panic!("Cannot convert {:?} to f64", self),
        }
    }
}

impl FromData<F32> for Data {
    fn from_data(&self) -> F32 {
        match self {
            Data::Float32(a_f32) => *a_f32,
            _ => panic!("Cannot convert {:?} to f32", self),
        }
    }
}

impl FromData<String> for Data {
    fn from_data(&self) -> String {
        match self {
            Data::String(a_string) => a_string.clone(),
            _ => panic!("Cannot convert {:?} to String", self),
        }
    }
}

impl FromData<Vec<u8>> for Data {
    fn from_data(&self) -> Vec<u8> {
        match self {
            Data::Bytes(a_bytes) => a_bytes.clone(),
            _ => panic!("Cannot convert {:?} to Vec<u8>", self),
        }
    }
}

pub trait AsData {
    fn as_data(&self) -> Data;
    fn data_hash(&self) -> [u8; 32];
}

impl AsData for bool {
    fn as_data(&self) -> Data {
        Data::Bool(*self)
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for String {
    fn as_data(&self) -> Data {
        Data::String(self.clone())
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for &str {
    fn as_data(&self) -> Data {
        Data::String(self.to_string())
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for str {
    fn as_data(&self) -> Data {
        Data::String(self.to_string())
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for usize {
    fn as_data(&self) -> Data {
        Data::UInt(*self)
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for F64 {
    fn as_data(&self) -> Data {
        Data::Float64(*self)
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for F32 {
    fn as_data(&self) -> Data {
        Data::Float32(*self)
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for Vec<u8> {
    fn as_data(&self) -> Data {
        Data::Bytes(self.clone())
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}

impl AsData for Data {
    fn as_data(&self) -> Data {
        self.clone()
    }
    fn data_hash(&self) -> [u8; 32] {
        blake_3(&self.as_data().as_bytes())
    }
}
