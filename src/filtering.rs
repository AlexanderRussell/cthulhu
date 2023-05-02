use crate::tentable::*;
use rayon::prelude::*;
pub trait FilterRows {
    fn eq(&self, column_index: usize, values: Vec<&str>) -> Vec<Row>;
    fn eq_first(&self, column_index: usize, values: Vec<&str>) -> Row;
    fn eq_any(&self, column_index: usize, values: Vec<&str>) -> Row;
    fn ne(&self, column_index: usize, values: Vec<&str>) -> Vec<Row>;
    fn ne_first(&self, column_index: usize, values: Vec<&str>) -> Row;
    fn ne_any(&self, column_index: usize, values: Vec<&str>) -> Row;
    fn contains(&self, column_index: usize, values: Vec<&str>) -> Vec<Row>;
}

impl FilterRows for Vec<Row> {
    fn eq(&self, column_index: usize, values: Vec<&str>) -> Vec<Row> {
        self.par_iter()
            .filter(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                values.contains(&value.as_str())
            })
            .map(|row| row.clone())
            .collect()
    }

    fn eq_first(&self, column_index: usize, values: Vec<&str>) -> Row {
        self.par_iter()
            .find_first(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                values.contains(&value.as_str())
            })
            .unwrap()
            .clone()
    }

    fn eq_any(&self, column_index: usize, values: Vec<&str>) -> Row {
        self.par_iter()
            .find_any(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                values.contains(&value.as_str())
            })
            .unwrap()
            .clone()
    }

    fn ne(&self, column_index: usize, values: Vec<&str>) -> Vec<Row> {
        self.par_iter()
            .filter(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                !values.contains(&value.as_str())
            })
            .map(|row| row.clone())
            .collect()
    }

    fn ne_first(&self, column_index: usize, values: Vec<&str>) -> Row {
        self.par_iter()
            .find_first(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                !values.contains(&value.as_str())
            })
            .unwrap()
            .clone()
    }

    fn ne_any(&self, column_index: usize, values: Vec<&str>) -> Row {
        self.par_iter()
            .find_any(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                !values.contains(&value.as_str())
            })
            .unwrap()
            .clone()
    }

    fn contains(&self, column_index: usize, values: Vec<&str>) -> Vec<Row> {
        self.par_iter()
            .filter(|row| {
                let row = row.read();
                let value = row.get(column_index).unwrap();
                values.iter().any(|x| value.contains(x))
            })
            .map(|row| row.clone())
            .collect()
    }
}
