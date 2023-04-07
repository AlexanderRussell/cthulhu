#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release
use chrono::{NaiveTime};
use cthulhu::table::*;
use hashbrown::HashMap;
use mimalloc::MiMalloc;
use regex::Regex;
use std::collections::BTreeMap;
use std::env;
use std::time::Instant;
use xlsxwriter::Workbook;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Uses regex to find the ticket number in the input string if 
/// it exists. 
fn extract_ticket_number(input: &str) -> Option<String> {
    let re = Regex::new(r#"(?i)INC\d{7}"#).unwrap();
    if let Some(matched) = re.find(input) {
        Some(matched.as_str().to_string())
    } else {
        None
    }
}

fn time_diff(start: &str, end: &str) -> Option<String> {
    let start_time = NaiveTime::parse_from_str(start, "%I:%M %p").ok()?;
    let end_time = NaiveTime::parse_from_str(end, "%I:%M %p").unwrap();
    let diff = end_time - start_time;
    let hours = diff.num_hours();
    let minutes = diff.num_minutes() % 60;
    Some(format!("{}h {}m", hours, minutes))
}

fn main() {
    let full_start = Instant::now();
    let start = Instant::now();
    let args = env::args().collect::<Vec<String>>();
    let file_path = args[1].to_owned();
    let mut buddy_file_path = file_path.clone();
    let mut report_file_path = file_path.clone();
    if file_path.contains(".csv") {
        buddy_file_path = buddy_file_path.replace(".csv", "_buddy.csv");
        report_file_path = report_file_path.replace(".csv", "_report.xlsx");
    } else {
        buddy_file_path = buddy_file_path.replace(".xlsx", "_buddy.xlsx");
    }
    let table = read_csv_to_table(&file_path, Some(2)).unwrap();
    let search = table.search_eq("", "Total for Agent:");
    let mut search = table.sort_rows_by_column(search, "Duration");
    search.reverse();
    let mut top_ten = Vec::new();
    for row in search.iter().take(9) {
        top_ten.push(*row);
    }
    let end = start.elapsed();
    println!("Time elapsed reading sys issue file is: {:?}", end);
    let start = Instant::now();
    let buddy_table = read_csv_to_table(&buddy_file_path, Some(2)).unwrap();
    let end = start.elapsed();
    println!("Time elapsed reading buddy schedule is: {:?}", end);
    let mut names_vec = Vec::new();
    let mut name_duration_map = BTreeMap::new();
    for row in top_ten {
        let name = table.get_value("Agent", row);
        let duration = table.get_value("Duration", row);
        if let Some(name) = name {
            let name = unsafe { &*name };
            names_vec.push(name.as_str());
            if let Some(duration) = duration {
                let duration = unsafe { &*duration };
                name_duration_map.insert(duration, name);
            }
        }
    }
    let columns_clone = buddy_table.get_columns().clone();

    // println!("columns: {:#?}", columns_clone);
    //fixing the dates in the buddy table
    let mut tables = HashMap::new();
    // println!("names vec: {:#?}", names_vec  );
    for name in &names_vec {
        let mut table = Table::new();
        table.import_columns(&columns_clone.clone());
        tables.insert(name.to_string(), table);
    }
    for row in buddy_table.get_all_rows() {
        let name = buddy_table.get_value("Agent", row);
        if let Some(name) = name {
            let name = unsafe { &*name };
            if let Some(table) = tables.get_mut(name) {
                table.add_row(row.clone());
            }
            // table.add_row(row.clone());
        }
    }

    // adding duration into tables
    for (_name, table) in &mut tables {
        table.add_column("Duration".to_string());
        let mut i = 0;
        loop {
            if let Some(row) = table.get_row(i) {
                let start_time = table.get_value("Start Time", row);
                let end_time = table.get_value("End Time", row);
                if let Some(start_time) = start_time {
                    let start_time = unsafe { &*start_time };
                    if let Some(end_time) = end_time {
                        let end_time = unsafe { &*end_time };
                        let duration = time_diff(start_time, end_time);
                        // table.set_value("Duration", row, duration);
                        if let Some(duration) = duration {
                            table.set_value("Duration", row, duration);
                        }
                    }
                }
                i += 1;
            } else {
                break;
            }
        }
    }

    for (_, table) in &mut tables {
        *table = table.create_sub_table(vec![
            "Agent",
            "Date",
            "Start Time",
            "End Time",
            "Duration",
            "Schedule State",
        ]);
    }

    // adding dates into tables
    for (_name, table) in &mut tables {
        // println!("this person {} has table columns: {:?}",name, table.get_columns());
        let mut i = 0;
        let mut current_date = String::new();
        loop {
            // println!("i: {}", i);
            if let Some(row) = table.get_row(i) {
                // println!("column: {:?}", table.get_columns());
                // println!("row: {:?}", row);
                let date = table.get_value("Date", row);
                // println!("any date? {:?}", date);
                if let Some(date) = date {
                    let date = unsafe { &*date };
                    // println!("date: {}", date);
                    if date == "" {
                        table.set_value("Date", row, current_date.clone());
                    } else {
                        current_date = date.clone();
                    }
                }
                i += 1;
            } else {
                break;
            }
        }
    }

    // adding a SNOW URL into a table if it has the right INC
    for (_, table) in &mut tables {
        table.add_column("SNOW URL".to_string());
        let mut i = 0;
        loop {
            if let Some(row) = table.get_row(i) {
                let schedule_state = table.get_value("Schedule State", row);
                if let Some(schedule_state) = schedule_state {
                    let schedule_state = unsafe { &*schedule_state };
                    if let Some(ticket_number) = extract_ticket_number(schedule_state) {
                        let url = format!("https://anztech.service-now.com/nav_to.do?uri=%2F$sn_global_search_results.do%3Fsysparm_search%3D{}", ticket_number);
                        table.set_value("SNOW URL", row, url);
                    }
                }
                i += 1;
            } else {
                break;
            }
        }
    }

    for (_, table) in &mut tables {
        let buddy_rows = table.get_all_rows();
        let buddy_rows =
            table.search_rows_contains("Schedule State", vec!["System Issue"], buddy_rows);
        table.retain(table.clone_rows(buddy_rows));
    }
    let mut report_workbook = Workbook::new(&report_file_path).unwrap();
    let mut duration_sheet = report_workbook.add_worksheet(Some("Duration")).unwrap();
    let header = vec!["Duration", "Agent"];
    for (i, header_cell) in header.iter().enumerate() {
        duration_sheet
            .write_string(0, i.try_into().unwrap(), header_cell, None)
            .unwrap();
    }
    for (j, (duration, agent)) in name_duration_map.iter().rev().enumerate() {
        duration_sheet
            .write_string(j as u32 + 1, 0, duration, None)
            .unwrap();
        duration_sheet
            .write_string(j as u32 + 1, 1, agent, None)
            .unwrap();
    }

    for (_, agent) in name_duration_map.iter().rev() {
        let table = tables.get_mut(*agent).unwrap();
        // let mut workbook = Workbook::new(format!("{}.xlsx", agent)).unwrap();
        write_table_to_xlsx(&table, Some(agent), &mut report_workbook).unwrap();
        // workbook.close().unwrap();
    }
    report_workbook.close().unwrap();
    let full_end = full_start.elapsed();
    println!("Total time elapsed is: {:?}", full_end);
    println!("Press any key to exit...");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}
