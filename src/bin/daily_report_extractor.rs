#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release
use chrono::prelude::{DateTime as DT, Local};
use chrono::{Duration, NaiveTime, Timelike};
use cthulhu::table::*;
// use hashbrown::HashMap;
use mimalloc::MiMalloc;
use std::env;
use std::error::Error;
// use regex::Regex;
// use std::collections::BTreeMap;
// use std::env;
// use std::time::Instant;
use xlsxwriter::*;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn get_date_string() -> String {
    let local: DT<Local> = Local::now();
    local.format("%d-%m-%y").to_string()
}

fn is_over_an_hour(time_str: &str) -> bool {
    let time = NaiveTime::parse_from_str(time_str, "%H:%M").unwrap();
    let duration = Duration::seconds(time.num_seconds_from_midnight() as i64);
    let minutes = duration.num_minutes();
    minutes >= 60
}

pub fn write_table_to_xlsx(
    table: &Table,
    name: Option<&str>,
    workbook: &mut Workbook,
) -> Result<(), Box<dyn Error>> {
    let mut worksheet = workbook.add_worksheet(name)?;
    let mut row = 0;
    for (index, name) in table.get_columns().iter() {
        worksheet.write_string(row, *index as u16, name, None)?;
    }
    row += 1;
    for row_data in table.get_data().iter() {
        let row_data = row_data.read();
        for (index, value) in row_data.iter().enumerate() {
            worksheet.write_string(
                row,
                index as u16,
                value,
                Some(
                    &workbook
                        .add_format()
                        .set_bg_color(xlsxwriter::FormatColor::Yellow),
                ),
            )?;
        }
        row += 1;
    }
    Ok(())
}

fn main() {
    let args = env::args().collect::<Vec<String>>();
    let file_path = args[1].to_owned();
    // let report_file_path = file_path.clone();
    let report_suffix = format!("{}_report.xlsx", get_date_string());
    // let report_file_path = report_file_path.replace(".csv", &report_suffix);
    let table = read_csv_to_table(&file_path, Some(2)).unwrap();
    let search = table.search_eq("", "Total for Agent:");
    let mut staff_over_an_hour = Vec::new();
    for row in search.iter() {
        let duration = table.get_value("Duration", row).unwrap();
        if is_over_an_hour(duration) {
            let agent_name = table.get_value("Agent", row).unwrap();
            staff_over_an_hour.push(agent_name.to_string());
        }
    }
    let staff_over_an_hour: Vec<&str> = staff_over_an_hour.iter().map(|x| x.as_str()).collect();
    let mut table = table.create_sub_table(vec![
        "Agent",
        "Date",
        "Start Time",
        "End Time",
        "Duration",
        "",
        "Schedule State",
    ]);
    let rows = table.get_all_rows();
    let rows = table.search_rows_contains("Agent", staff_over_an_hour, rows);
    table.retain(table.clone_rows(rows));
    let mut report_workbook = Workbook::new(&report_suffix).unwrap();
    write_table_to_xlsx(&table, Some(&get_date_string()), &mut report_workbook).unwrap();
    report_workbook.close().unwrap();
}
