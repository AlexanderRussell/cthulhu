#![allow(dead_code, unused_imports)]
use crate::data::*;
use parking_lot::Mutex;
use rayon::current_num_threads;
use rayon::current_thread_index;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
pub type Row = Vec<String>;

#[derive(Debug)]
pub struct Table {
    columns: HashMap<usize, String>,
    data: Vec<Row>,
}

impl Table {
    pub fn new() -> Self {
        Table {
            columns: HashMap::new(),
            data: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column_name: String) {
        let column_index = self.columns.len();
        self.columns.insert(column_index, column_name);
        for row in &mut self.data {
            row.push(String::new());
        }
    }

    pub fn add_row(&mut self, row: Row) {
        if self.columns.is_empty() {
            for (index, _) in row.iter().enumerate() {
                self.columns.insert(index, String::new());
            }
        }
        self.data.push(row);
    }

    pub fn get_value<'t>(&'t self, field: &str, row: &'t Row) -> Option<&String> {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == field {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            row.get(column_index)
        } else {
            None
        }
    }

    pub fn search_rows_contains<'t>(
        &'t self,
        column_name: &str,
        values: Vec<&str>,
        rows: Vec<&'t Row>,
    ) -> Vec<&Row> {
        let threads = current_num_threads();
        let mut handles = Vec::new();
        for _ in 0..threads {
            handles.push(Arc::new(Mutex::new(Vec::new())));
        }
        rows.par_iter().for_each(|row| {
            if let Some(value) = self.get_value(column_name, row) {
                for a_value in &values {
                    if a_value.contains(value) {
                        let index = current_thread_index().unwrap();
                        handles[index].lock().push(*row);
                    }
                }
            }
        });
        let mut ret_vec = Vec::new();
        for handle in handles {
            let mut handle = handle.lock();
            ret_vec.append(&mut handle);
        }

        ret_vec
    }

    pub fn search_eq(&self, column_name: &str, value: &str) -> Vec<&Row> {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            self.data
                .par_iter()
                .filter(|row| row.get(column_index) == Some(&value.to_owned()))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn search_eq_many(&self, column_name: &str, values: Vec<&str>) -> Vec<&Row> {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            self.data
                .par_iter()
                .filter(|row| {
                    if let Some(value) = row.get(column_index) {
                        values.contains(&value.as_str())
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn search_ne(&self, column_name: &str, value: &str) -> Vec<&Row> {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            self.data
                .par_iter()
                .filter(|row| row.get(column_index) != Some(&value.to_owned()))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_row_as_map(&self, row: Row) -> HashMap<String, String> {
        let mut row_map = HashMap::new();
        for (index, value) in row.into_iter().enumerate() {
            if let Some(column_name) = self.columns.get(&index) {
                row_map.insert(column_name.to_owned(), value);
            }
        }
        row_map
    }

    pub fn sort_by_column(&mut self, column_name: &str) {
        if let Some(column_index) = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        }) {
            self.data.sort_by(|row1, row2| {
                let def = String::new();
                let value1 = row1.get(column_index).unwrap_or(&def);
                let value2 = row2.get(column_index).unwrap_or(&def);
                value1.partial_cmp(value2).unwrap_or(Ordering::Equal)
            });
        }
    }

    pub fn sort_rows_by_column<'row>(
        &self,
        mut rows: Vec<&'row Row>,
        column_name: &str,
    ) -> Vec<&'row Row> {
        if let Some(column_index) = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        }) {
            rows.sort_by(|row1, row2| {
                let def = String::new();
                let value1 = row1.get(column_index).unwrap_or(&def);
                let value2 = row2.get(column_index).unwrap_or(&def);
                value1.partial_cmp(value2).unwrap_or(Ordering::Equal)
            });
        };
        rows
    }
}

pub fn read_csv_to_table(file_path: &str, skip: Option<usize>) -> Result<Table, Box<dyn Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(file_path)?;
    let mut column_record = HashMap::new();
    let header = rdr
        .records()
        .nth(skip.unwrap_or(0))
        .ok_or("no row found after skip")?;
    let header = match header {
        Ok(header) => header,
        Err(_) => Err("no header row found")?,
    };
    for (i, field) in header.iter().enumerate() {
        column_record.insert(i, field.to_owned());
    }
    let mut table = Table::new();
    for (i, field) in column_record.iter() {
        table.columns.insert(*i, field.to_owned());
    }
    for result in rdr.records() {
        let record = result?;
        let mut row = Vec::new();
        for field in record.iter() {
            row.push(field.to_owned());
        }
        table.data.push(row);
    }
    Ok(table)
}

pub fn read_csv(file_path: &str, skip: Option<usize>) -> Result<Table, Box<dyn Error>> {
    let file = File::open(file_path).expect("bad file");
    let mut lines = BufReader::new(file).lines();

    // Read the first line as the column names
    let column_names: Row = lines
        .nth(skip.unwrap_or(0))
        .expect("dun goofed no utf8???")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect();

    // Create a new table with the column names
    let mut table = Table::new();
    for column_name in column_names {
        table.add_column(column_name);
    }

    // Read the remaining lines as data rows
    for line in lines.enumerate() {
        // println!("bad line is {:?}", line.0);
        let row: Row = line
            .1
            .expect("bad line")
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();
        table.add_row(row);
    }

    Ok(table)
}

mod test {

    use super::*;

    #[test]
    fn test_read_csv() {
        let table = read_csv("feb.csv", Some(2)).unwrap();
        let search = table.search_eq("", "Total for Agent:");
        let mut search = table.sort_rows_by_column(search, "Duration");
        search.reverse();
        let mut top_ten = Vec::new();
        for row in search.iter().take(9) {
            top_ten.push(*row);
        }
        println!("{:#?}", top_ten);
    }
}
