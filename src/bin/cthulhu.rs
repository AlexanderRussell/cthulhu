use cthulhu::table::*;
use std::time::Instant;

fn main() {
    let start = Instant::now();
    let table = read_csv("feb.csv", Some(2)).unwrap();
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
    let buddy_table = read_csv_to_table("feb_buddy.csv", Some(2)).unwrap();
    let end = start.elapsed();
    println!("Time elapsed reading buddy schedule is: {:?}", end);
    let mut names_vec = Vec::new();
    for row in top_ten {
        let name = table.get_value("Agent", row);
        if let Some(name) = name {
            names_vec.push(name.as_str());
        }
    }
    let sought_buddy_rows = buddy_table.search_eq_many("Agent", names_vec);
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}
