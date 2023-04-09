#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release
use chrono::NaiveTime;
use cthulhu::table::*;
use hashbrown::HashMap;
use mimalloc::MiMalloc;
use regex::Regex;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::time::Instant;
use xlsxwriter::Workbook;

fn main() {

    // this reporting tool will create reports based upon the option
    // given in the run args
}
