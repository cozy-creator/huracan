use std::str::FromStr;
use regex::Regex;
use sui_types::base_types::ObjectType;


/// ranges are inclusive on both sides
pub fn make_descending_ranges(mut numbers: Vec<u64>) -> Vec<(u64, u64)> {
	if numbers.is_empty() {
		return Vec::new()
	}
	numbers.sort_unstable();
	let mut start = numbers[0];
	let mut end = numbers[0];
	let mut ranges = Vec::new();
	for &n in numbers.iter().skip(1) {
		if n == end + 1 {
			end = n;
		} else {
			ranges.push((end, start));
			start = n;
			end = n;
		}
	}
	ranges.push((end, start));
	ranges.reverse();
	ranges
}

/// Check if a string matches any regex expression in a vector of regex expressions.
///
/// # Arguments
///
/// * `input` - The string to be checked.
/// * `regex_vec` - A vector of regex expressions to match against.
///
/// # Returns
///
/// * `bool` - Returns `true` if the string matches any regex expression, `false` otherwise.
pub(crate) fn check_string_against_regex(input_obj_type: &ObjectType, obj_type_string_vec: Vec<String>) -> bool {
	for item in obj_type_string_vec {
		let item_obj_type = ObjectType::from_str(&item).Ok();
		if input_obj_type == item_obj_type {
			return true;
		}
		// let regex = Regex::new(&*regex_str).unwrap();
		// if regex.is_match(input) {
		// 	return true;
		// }
	}
	false
}

#[cfg(test)]
mod test {
	use crate::utils::make_descending_ranges;

	#[test]
	fn test_make_descending_ranges() {
		let res = make_descending_ranges(vec![3, 1, 2, 7, 9, 11, 10]);
		assert_eq!(res, vec![(11, 9), (7, 7), (3, 1)]);
	}
}
