use parking_lot::RwLock;
// use parking_lot::Mutex;
// use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::error::Error;
use chrono::Utc;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use xlsxwriter::Workbook;
use serde::{Deserialize, Serialize};
use crate::filtering::FilterRows;

pub type Row = Arc<RwLock<Vec<String>>>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ShardID {
    pub id: usize,
    pub shards: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Table {
    name: Option<String>,
    latest_row: usize,
    columns: BTreeMap<usize, String>,
    shard: Option<ShardID>,
    timestamps: HashMap<usize, i64>,
    data: HashMap<usize, Row>,
    
    // timestamps: 
}

impl Table {
    pub fn new() -> Self {
        Table {
            name: None,
            // columns: HashMap::new(),
            columns: BTreeMap::new(),
            // data: Vec::new(),
            data: HashMap::new(),
            latest_row: 0,
            shard: None,
            // timestamps: Vec::new(),
            timestamps: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }


    pub fn save_to_bytes(&self, file_path: &str) -> Result<(), Box<dyn Error>> {
        // let mut bytes = Vec::new();
        let bytes = serde_json::to_vec(self)?;
        std::fs::write(file_path, bytes)?;
        Ok(())
    }

    pub fn read_from_bytes(file_path: &str) -> Result<Self, Box<dyn Error>> {
        let bytes = std::fs::read(file_path)?;
        let table: Table = serde_json::from_slice(&bytes)?;
        Ok(table)
    }

    // pub fn to_shards(self, shards: usize) -> Result<Vec<Table>, Box<dyn Error>> {
    //     if self.shard.is_some() {
    //         let shard_error = format!("Table {} is already sharded", self.name.unwrap_or("UNNAMED".to_string()));
    //         return Err(shard_error.into());
    //     }
    //     let mut tables = Vec::new();
    //     let split = self.data.len();
    //     let mut shard_id = 1;
    //     let row_shards: Vec<Vec<Row>> = self
    //         .data
    //         .into_values()
    //         .collect::<Vec<Row>>()
    //         .chunks(split / shards)
    //         .map(|row| row.into())
    //         .collect();
    //     // let schema = self.schema.clone();
    //     let latest_nat_key = self.latest_row;
    //     for rows in row_shards {
    //         let mut table = Table::new();
    //         for row in rows {
    //             table.data.insert(row.get_row_id(), row.clone());
    //         }
    //         table.shard = Some(ShardID {
    //             id: shard_id,
    //             shards,
    //         });
    //         // removing the shard amount from the natural key means no natural key space is lost
    //         table.latest_row_id = latest_nat_key + shard_id - shards;
    //         tables.push(table);
    //         shard_id += 1;
    //     }
    //     Ok(tables)
    // }

    pub fn to_shards(self, shards: usize) -> Result<Vec<Table>, Box<dyn Error>> {
        if self.shard.is_some() {
            let shard_error = format!("Table {} is already sharded", self.name.unwrap_or("UNNAMED".to_string()));
            return Err(shard_error.into());
        }
        let columns = self.columns.clone();
        let mut result = vec![Table::new(); shards];
        for (i, item) in self.data.into_iter() {
            result[i % shards].data.insert(i, item);
        }
        for (i, table) in result.iter_mut().enumerate() {
            table.shard = Some(ShardID {
                id: i + 1,
                shards,
            });
            table.columns = columns.clone();
        }
        Ok(result)
    }
    

    pub fn from_shards(tables: Vec<Table>) -> Result<Table, Box<dyn Error>> {
        let mut new_table = Table::new();
        if tables.len() == 0 {
            return Ok(new_table);
        }
        new_table.columns = tables[0].columns.clone();
        // looping through the tables to get each value from key 1..n
        for table in tables {
            for (key, value) in table.data {
                new_table.data.insert(key, value);
            }
        }
        new_table.shard = None;
        Ok(new_table)
    }

    pub fn get_data(&self) -> &HashMap<usize, Row> {
        &self.data
    }

    /// Adds a new column to the `Table`.
    pub fn add_column(&mut self, column_name: String) {
        let column_index = self.columns.len();
        self.columns.insert(column_index, column_name);
        for (_index,row) in &mut self.data {
            row.write().push(String::new());
        }
    }

    pub fn create_sub_table(&self, columns: Vec<&str>) -> Table {
        let mut sub_table = Table::new();
        for column in &columns {
            sub_table.add_column(column.to_string());
        }
        for (_index,row) in &self.data {
            let new_row = Row::new(RwLock::new(Vec::new()));
            for column in &columns {
                let column_index = self.field_to_index(column);
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

    pub fn into_sub_table(&mut self, columns: Vec<&str>) {
        let mut sub_table = Table::new();
        for column in &columns {
            sub_table.add_column(column.to_string());
        }
        for (_index,row) in &self.data {
            let new_row = Row::new(RwLock::new(Vec::new()));
            for column in &columns {
                let column_index = self.columns.iter().find_map(
                    |(index, name)| {
                        if name == column {
                            Some(*index)
                        } else {
                            None
                        }
                    },
                );
                if let Some(column_index) = column_index {
                    let read = row.read();
                    let value = read.get(column_index).unwrap();
                    new_row.write().push(value.clone());
                }
            }
            sub_table.add_row(new_row);
        }
        self.columns = sub_table.columns;
        self.data = sub_table.data;
    }

    pub fn get_columns(&self) -> &BTreeMap<usize, String> {
        &self.columns
    }

    pub fn import_columns(&mut self, columns: &BTreeMap<usize, String>) {
        self.columns = columns.clone();
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) {
        let column_index = self.field_to_index(old_name);
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
        match &self.shard {
            Some(shard) => self.latest_row += shard.shards,
            None => {
                self.latest_row += 1;
            }
        }
        self.timestamps.insert(self.latest_row, Utc::now().timestamp_millis());
        self.data.insert(self.latest_row, row);
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
        let mut new_data = HashMap::new();
        let rows: Vec<Vec<String>> = rows.iter().map(|row| row.read().clone()).collect();
        for (index,row) in &self.data {
            let row = row.read().clone();
            if rows.contains(&row) {
                new_data.insert(*index,Arc::new(RwLock::new(row)));
            }
        }
        self.data = new_data;
    }

    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.data.get(&index)
    }

    /// Returns a value of a row at a given column field. Returns None if the field is not found,
    /// or a String ref if the field is found.
    /// This is done to avoid cloning, as well as get around the borrow checker being
    /// mad that I am trying to return a ref to a 'temporary' value.
    pub fn get_value<'t>(&'t self, field: &str, row: &'t Row) -> Option<&String> {
        let column_index = self.field_to_index(field);
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
        let column_index =
            self.columns.iter().find_map(
                |(index, name)| {
                    if name == field {
                        Some(*index)
                    } else {
                        None
                    }
                },
            );
        if let Some(column_index) = column_index {
            row.write()[column_index] = value;
        }
    }

    pub fn get_all_rows(&self) -> Vec<Row> {
        let blank_row = Row::new(RwLock::new(Vec::new()));
        let mut rows = vec![blank_row; self.data.len()];
        for (index, row) in &self.data {
            rows[*index-1] = row.clone();
        }
        rows

    }
    pub fn get_all_rows_as_index_map(&self) -> HashMap<usize, Row> {
        self.data.clone()
    }

    pub fn index_to_field(&self, index: usize) -> Option<&str> {
        self.columns.get(&index).map(|s| s.as_str())
    }

    pub fn field_to_index(&self, field: &str) -> Option<usize> {
        self.columns.iter().find_map(
            |(index, name)| {
                if name == field {
                    Some(*index)
                } else {
                    None
                }
            },
        )
    }
    #[inline]
    pub fn search_rows_contains(
        &self,
        column_name: &str,
        values: Vec<&str>,
    ) -> Vec<Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.get_all_rows().contains(column_index, values)
        } else {
            Vec::new()
        }
    }
    #[inline]
    pub fn search_eq(&self, column_name: &str, values: Vec<&str>) -> Vec<Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.get_all_rows().eq(column_index, values)
        } else {
            Vec::new()
        }
    }
    #[inline]
    pub fn search_ne(&self, column_name: &str, values: Vec<&str>) -> Vec<Row> {
        let column_index = self.field_to_index(column_name);
        if let Some(column_index) = column_index {
            self.get_all_rows().ne(column_index, values)
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

    // pub fn sort_by_column(&mut self, column_name: &str) {
    //     if let Some(column_index) = self.columns.iter().find_map(|(index, name)| {
    //         if name == column_name {
    //             Some(*index)
    //         } else {
    //             None
    //         }
    //     }) {
    //         self.data.sort_by(|row1, row2| {
    //             let def = String::new();
    //             let read_1 = row1.read();
    //             let read_2 = row2.read();
    //             let value1 = read_1.get(column_index).unwrap_or(&def);
    //             let value2 = read_2.get(column_index).unwrap_or(&def);
    //             value1.partial_cmp(value2).unwrap_or(Ordering::Equal)
    //         });
    //     }
    // }

    pub fn sort_rows_by_column(&self, mut rows: Vec<Row>, column_name: &str) -> Vec<Row> {
        if let Some(column_index) = self.columns.iter().find_map(|(index, name)| {
            if name == column_name {
                Some(*index)
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
    for (index, name) in table.columns.iter() {
        worksheet.write_string(row, *index as u16, name, None)?;
    }
    row += 1;
    for (_,row_data) in table.data.iter() {
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
    let mut row_index = 1;
    for result in rdr.records() {
        let record = result?;
        let mut row = Vec::new();
        for field in record.iter() {
            row.push(field.to_owned());
        }
        let row = Arc::new(RwLock::new(row));
        table.data.insert(row_index, row);
        table.timestamps.insert(row_index, Utc::now().timestamp_millis());
        table.latest_row = row_index;
        row_index += 1;
    }
    Ok(table)
}

pub fn read_csv(file_path: &str, skip: Option<usize>) -> Result<Table, Box<dyn Error>> {
    let file = File::open(file_path).expect("unable to open file");
    let mut lines = BufReader::new(file).lines();
    let column_names: Vec<String> = lines
        .nth(skip.unwrap_or(0))
        .expect("invalid UTF-8 in csv file!")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect();
    let mut table = Table::new();
    for column_name in column_names {
        table.add_column(column_name);
    }
    for line in lines.enumerate() {
        let row: Vec<String> = line
            .1
            .expect("error while reading line")
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();
        let row = Arc::new(RwLock::new(row));
        table.add_row(row);
        table.latest_row += 1;
    }
    Ok(table)
}




#[cfg(test)]
mod tests {

    // use std::time::Instant;
    use super::*;

    #[test]
    fn sharding_table() {
        let table = read_csv_to_table("feb_buddy.csv", Some(2)).unwrap();
        let table_len = table.len();
        let shards = table.to_shards(14).unwrap();
        assert_eq!(shards.len(), 14);
        println!("shards");
        let recreated_table = Table::from_shards(shards).unwrap();
        let recreated_table_len = recreated_table.len();
        assert_eq!(table_len, recreated_table_len);
        println!("recreated table");
        // recreated_table.save_to_json("second.json").unwrap();
        recreated_table.save_to_bytes("table.bytes").unwrap();
        println!("saved table");
        // let loaded_recreated_table = Table::read_from_json("second.json").unwrap();
        let loaded_recreated_table = Table::read_from_bytes("table.bytes").unwrap();
        assert_eq!(table_len, loaded_recreated_table.len());        
        
    }

   
    

  

}