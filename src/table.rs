#![allow(unused_imports)]
// use crate::data::*;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rayon::current_num_threads;
use rayon::current_thread_index;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ptr::eq;
use std::sync::Arc;
use xlsxwriter::Workbook;

use crate::filtering::FilterRows;
pub type Row = Arc<RwLock<Vec<String>>>;

#[derive(Debug)]
pub struct Table {
    columns: HashMap<usize, String>,
    data: Vec<Row>,
    // rows: HashMap<usize, Row>,
}

impl Table {
    pub fn new() -> Self {
        Table {
            columns: HashMap::new(),
            data: Vec::new(),
            // rows: HashMap::new(),
        }
    }

    pub fn get_data(&self) -> &Vec<Row> {
        &self.data
    }

    // pub fn get_rows(&self) -> &HashMap<usize, Row> {
    //     &self.rows
    // }

    /// Adds a new column to the `Table`.
    pub fn add_column(&mut self, column_name: String) {
        let column_index = self.columns.len();
        self.columns.insert(column_index, column_name);
        for row in &mut self.data {
            row.write().push(String::new());
        }
    }

    pub fn create_sub_table(&self, columns: Vec<&str>) -> Table {
        let mut sub_table = Table::new();
        for column in &columns {
            sub_table.add_column(column.to_string());
        }
        for row in &self.data {
            let new_row = Row::new(RwLock::new(Vec::new()));
            for column in &columns {
                let column_index = self.columns.iter().find_map(|(index, name)| {
                    if name == column {
                        Some(index.clone())
                    } else {
                        None
                    }
                });
                if let Some(column_index) = column_index {
                    let read = row.read();
                    let value = read.get(column_index).unwrap();
                    new_row.write().push(value.clone());
                }
            }
            sub_table.add_row(new_row);
        }
        sub_table
    }

    pub fn get_columns(&self) -> &HashMap<usize, String> {
        &self.columns
    }

    pub fn import_columns(&mut self, columns: &HashMap<usize, String>) {
        self.columns = columns.clone();
    }

    pub fn reame_column(&mut self, old_name: &str, new_name: &str) {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == old_name {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            self.columns.insert(column_index, new_name.to_string());
        }
    }

    pub fn add_row(&mut self, row: Row) {
        if self.columns.is_empty() {
            for (index, _) in row.write().iter().enumerate() {
                self.columns.insert(index, String::new());
            }
        }
        // self.rows.insert(self.rows.len(), row.clone());
        self.data.push(row);
    }

    // this is dumb and stupid
    pub fn clone_rows(&self, rows: Vec<&Row>) -> Vec<Row> {
        let mut new_rows = Vec::new();
        for row in rows {
            let new_row = Row::new(RwLock::new(row.read().clone()));
            new_rows.push(new_row);
        }
        new_rows
    }

    pub fn retain(&mut self, rows: Vec<Row>) {
        let mut new_data = Vec::new();
        let rows: Vec<Vec<String>> = rows.iter().map(|row| row.read().clone()).collect();
        for row in &self.data {
            let row = row.read().clone();
            if rows.contains(&row) {
                new_data.push(Arc::new(RwLock::new(row)));
            }
        }
        self.data = new_data;
    }

    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.data.get(index)
    }

    /// returns a value of a row at a given column field. Returns None if the field is not found,
    /// or a String pointer if the field is found.
    /// This is done to avoid cloning, as well as get around the borrow checker being
    /// mad that I am trying to return a ref to a 'temporary' value
    pub fn get_value<'t>(&'t self, field: &str, row: &'t Row) -> Option<&String> {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == field {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            let read = row.read();
            let the_value = read.get(column_index);
            if let Some(the_value) = the_value {
                let value_pointer = the_value as *const String;
                let value_pointer = unsafe { &*value_pointer };
                Some(value_pointer)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn set_value(&self, field: &str, row: &Row, value: String) {
        let column_index = self.columns.iter().find_map(|(index, name)| {
            if name == field {
                Some(index.clone())
            } else {
                None
            }
        });
        if let Some(column_index) = column_index {
            row.write()[column_index] = value;
        }
    }

    pub fn get_all_rows(&self) -> Vec<&Row> {
        self.data.iter().collect()
    }
    pub fn get_all_rows_as_index_map(&self) -> HashMap<usize, &Row> {
        let mut map = HashMap::new();
        for (index, row) in self.data.iter().enumerate() {
            map.insert(index, row);
        }
        map
    }

    pub fn index_to_field(&self, index: usize) -> Option<&str> {
        self.columns.get(&index).map(|s| s.as_str())
    }

    pub fn field_to_index(&self, field: &str) -> Option<usize> {
        self.columns.iter().find_map(|(index, name)| {
            if name == field {
                Some(index.clone())
            } else {
                None
            }
        })
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
                    // println!("comparing {} with {}", a_value, value);
                    if value.contains(a_value) {
                        let index = current_thread_index().unwrap();
                        handles[index].lock().push(*row);
                    }
                }
            }
        });
        let mut ret_vec = Vec::new();
        for handle in handles {
            let mut handle = handle.lock();
        }

        ret_vec
    }

    pub fn search_eq_old(&self, column_name: &str, value: &str) -> Vec<&Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.data
                .par_iter()
                .filter(|row| row.read().get(column_index) == Some(&value.to_owned()))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn search_eq(&self, column_name: &str, value: &str) -> Vec<Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.data
                .eq(column_index, vec![value])
        } else {
            Vec::new()
        }
    }

    pub fn search_eq_many(&self, column_name: &str, values: Vec<&str>) -> Vec<&Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.data
                .par_iter()
                .filter(|row| {
                    if let Some(value) = row.read().get(column_index) {
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
                .filter(|row| row.read().get(column_index) != Some(&value.to_owned()))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_row_as_map(&self, row: Row) -> HashMap<String, String> {
        let mut row_map = HashMap::new();
        for (index, value) in row.read().iter().enumerate() {
            if let Some(column_name) = self.columns.get(&index) {
                row_map.insert(column_name.to_owned(), value.to_owned());
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
                let read_1 = row1.read();
                let read_2 = row2.read();
                let value1 = read_1.get(column_index).unwrap_or(&def);
                let value2 = read_2.get(column_index).unwrap_or(&def);
                value1.partial_cmp(value2).unwrap_or(Ordering::Equal)
            });
        }
    }

    pub fn sort_rows_by_column(
        &self,
        mut rows: Vec<Row>,
        column_name: &str,
    ) -> Vec<Row> {
        if let Some(column_index) = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(index.clone())
            } else {
                None
            }
        }) {
            rows.sort_by(|row1, row2| {
                let def = String::new();
                let read_1 = row1.read();
                let read_2 = row2.read();
                let value1 = read_1.get(column_index).unwrap_or(&def);
                let value2 = read_2.get(column_index).unwrap_or(&def);
                value1.partial_cmp(value2).unwrap_or(Ordering::Equal)
            });
        };
        rows
    }
}

pub fn write_table_to_xlsx(
    table: &Table,
    name: Option<&str>,
    workbook: &mut Workbook,
) -> Result<(), Box<dyn Error>> {
    let mut worksheet = workbook.add_worksheet(name)?;
    let mut row = 0;
    // let mut col = 0;
    for (index, name) in table.columns.iter() {
        worksheet.write_string(row, *index as u16, name, None)?;
    }
    row += 1;
    for row_data in table.data.iter() {
        let row_data = row_data.read();
        for (index, value) in row_data.iter().enumerate() {
            worksheet.write_string(row, index as u16, value, None)?;
        }
        row += 1;
    }
    Ok(())
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
    let header = header?;
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
        let row = Arc::new(RwLock::new(row));
        table.data.push(row);
    }
    Ok(table)
}

pub fn read_csv(file_path: &str, skip: Option<usize>) -> Result<Table, Box<dyn Error>> {
    let file = File::open(file_path).expect("bad file");
    let mut lines = BufReader::new(file).lines();

    // Read the first line as the column names
    let column_names: Vec<String> = lines
        .nth(skip.unwrap_or(0))
        .expect("dun goofed no utf8???")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect();
    // let column_names = A
    // Create a new table with the column names
    let mut table = Table::new();
    for column_name in column_names {
        table.add_column(column_name);
    }

    // Read the remaining lines as data rows
    for line in lines.enumerate() {
        // println!("bad line is {:?}", line.0);
        let row: Vec<String> = line
            .1
            .expect("bad line")
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();
        let row = Arc::new(RwLock::new(row));
        table.add_row(row);
    }

    Ok(table)
}

mod test {
    use crate::table::*;
    // use super::*;

    // #[test]
    // fn test_read_csv() {
    //     let table = read_csv("feb.csv", Some(2)).unwrap();
    //     let search = table.search_eq("", "Total for Agent:");
    //     let mut search = table.sort_rows_by_column(search, "Duration");
    //     search.reverse();
    //     let mut top_ten = Vec::new();
    //     for row in search.iter().take(9) {
    //         top_ten.push(*row);
    //     }
    //     println!("{:#?}", top_ten);
    // }
}

// https://anztech.service-now.com/nav_to.do?uri=%2F$sn_global_search_results.do%3Fsysparm_search%3D
