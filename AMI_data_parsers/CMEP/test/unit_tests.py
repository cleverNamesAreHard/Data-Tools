import datetime
import parse_mepmd01 as pm
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from parse_mepmd01 import (
	parse_calculation_constant,
	parse_cmep_file,
	parse_commodity,
	parse_count,
	parse_interval,
	parse_interval_data,
	parse_purpose,
	parse_record,
	parse_record_type,
	parse_record_version,
	parse_time_stamp,
	parse_units,
)

import inspect


TEST_DIR = Path(__file__).parent

class TestParseMEPMD01(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		# Force the module to capture "now" once, using its own logic.
		pm._REFERENCE_NOW = None
		pm._REFERENCE_TODAY = None
		pm._REFERENCE_YEAR = None
		pm._ensure_reference_time()

	def _get_valid_row(self):
		return [
			"MEPMD01",
			"19970819",
			"Test Uploader",
			"ID1234",
			"Recipient",
			"ID5678",
			"202602251345",
			"M12345678",
			"OK",
			"W",
			"GAL",
			"",
			"00000100",
			"2",
			"202602251200",
			"R",
			"25",
			"",
			"R",
			"10"
		]

	def _get_valid_row_csv_line(self):
		return ",".join(self._get_valid_row()) + "\n"

	def _write_temp_csv(self, file_name, file_contents):
		temp_dir = tempfile.TemporaryDirectory()
		self.addCleanup(temp_dir.cleanup)

		file_path = Path(temp_dir.name) / file_name
		with open(file_path, "w", newline="", encoding="utf-8") as temp_file:
			temp_file.write(file_contents)

		return file_path

	# -------------------------------------------------------------------------
	# Column 1: Record Type (parse_record_type)
	# -------------------------------------------------------------------------

	# Test that a valid MEPMD01 record type is accepted and returned.
	# Input: "MEPMD01".
	# Expected: Returned unchanged.
	def test_parse_record_type_accepts_mepmd01(self):
		self.assertEqual(parse_record_type("MEPMD01", row_number=1), "MEPMD01")

	# Test that an invalid record type is rejected with row context.
	# Input: "MEPMD02", row_number=7.
	# Expected: ValueError mentioning expected MEPMD01 and row 7.
	def test_parse_record_type_rejects_invalid_with_row_context(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_type("MEPMD02", row_number=7)

		self.assertIn("Expected: MEPMD01", str(ctx.exception))
		self.assertIn("row 7", str(ctx.exception))

	# Test that a non-string Record Type is rejected with row context.
	# Input: None, row_number=3.
	# Expected: ValueError indicating expected string and row 3.
	def test_parse_record_type_rejects_invalid_type_with_row_context(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_type(None, row_number=3)

		self.assertIn("Expected string", str(ctx.exception))
		self.assertIn("row 3", str(ctx.exception))

	# Test that future Record Version dates within the current year are rejected.
	# Input: Tomorrow's date in YYYYMMDD format.
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_version_future_within_year_raises_value_error(self):
		tomorrow = pm._REFERENCE_TODAY + datetime.timedelta(days=1)
		future_value = tomorrow.strftime("%Y%m%d")

		with self.assertRaises(ValueError) as ctx:
			parse_record_version(future_value)

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# Test that a non-string Record Type is rejected without row context.
	# Input: None, row_number=None.
	# Expected: ValueError indicating expected string.
	def test_parse_record_type_rejects_invalid_type_without_row_context(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_type(None, row_number=None)

		self.assertIn("Expected string", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 2: Record Version (parse_record_version)
	# -------------------------------------------------------------------------

	# Test that a valid YYYYMMDD Record Version parses successfully.
	# Input: "19970819".
	# Expected: Parsed structure with format "YYYYMMDD" and a datetime.date release_date.
	def test_parse_record_version_valid_yyyymmdd_parses(self):
		result = parse_record_version("19970819")

		self.assertEqual(result["raw"], "19970819")
		self.assertEqual(result["format"], "YYYYMMDD")
		self.assertIsNone(result["version_number"])
		self.assertEqual(result["release_date"], datetime.date(1997, 8, 19))

	# Test that Record Version strips whitespace.
	# Input: "  19970819  ".
	# Expected: Successful parse with raw equal to stripped value.
	def test_parse_record_version_strips_whitespace(self):
		result = parse_record_version("  19970819  ")

		self.assertEqual(result["raw"], "19970819")
		self.assertEqual(result["format"], "YYYYMMDD")
		self.assertEqual(result["release_date"], datetime.date(1997, 8, 19))

	# Test that Record Version rejects invalid type.
	# Input: None.
	# Expected: ValueError indicating invalid Record Version type.
	def test_parse_record_version_invalid_type_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_version(None)

		self.assertIn("Invalid Record Version type", str(ctx.exception))

	# Test that Record Version must be fixed-width (8 chars).
	# Input: "260224" (6 chars).
	# Expected: ValueError indicating expected YYYYMMDD.
	def test_parse_record_version_invalid_length_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_version("260224")

		self.assertIn("Expected YYYYMMDD format", str(ctx.exception))

	# Test that non-numeric YYYYMMDD is rejected.
	# Input: "1997AA19".
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_version_non_numeric_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_version("1997AA19")

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# Test that invalid calendar dates are rejected.
	# Input: "19970231" (Feb 31, invalid).
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_version_invalid_calendar_date_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_version("19970231")

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# Test that future YYYYMMDD values are rejected.
	# Input: (current year + 1) + "0224".
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_version_future_date_raises_value_error(self):
		future_year = pm._REFERENCE_YEAR + 1
		future_value = f"{future_year}0224"

		with self.assertRaises(ValueError) as ctx:
			parse_record_version(future_value)

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# Test that CC-prefixed Record Version values are rejected by this parser.
	# Input: "T1260224".
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_version_cc_prefixed_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_record_version("T1260224")

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 7: Time Stamp (parse_time_stamp)
	# -------------------------------------------------------------------------

	# Test that a valid YYYYMMDDHHMM Time Stamp parses successfully.
	# Input: "202602241200" (2026-02-24 12:00).
	# Expected: Parsed structure with format "YYYYMMDDHHMM" and a datetime time_stamp.
	def test_parse_time_stamp_valid_yyyymmddhhmm_parses(self):
		result = parse_time_stamp("202602241200")

		self.assertEqual(result["raw"], "202602241200")
		self.assertEqual(result["format"], "YYYYMMDDHHMM")
		self.assertIsNone(result["version_number"])
		self.assertEqual(result["time_stamp"], datetime.datetime(2026, 2, 24, 12, 0))

	# Test that Time Stamp strips whitespace.
	# Input: "  202602241200  ".
	# Expected: Successful parse with raw equal to stripped value.
	def test_parse_time_stamp_strips_whitespace(self):
		result = parse_time_stamp("  202602241200  ")

		self.assertEqual(result["raw"], "202602241200")
		self.assertEqual(result["format"], "YYYYMMDDHHMM")
		self.assertEqual(result["time_stamp"], datetime.datetime(2026, 2, 24, 12, 0))

	# Test that Time Stamp rejects invalid type.
	# Input: None.
	# Expected: ValueError indicating invalid Time Stamp type.
	def test_parse_time_stamp_invalid_type_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp(None)

		self.assertIn("Invalid Time Stamp type", str(ctx.exception))

	# Test that Time Stamp must be fixed-width (12 chars).
	# Input: "20260224120" (11 chars).
	# Expected: ValueError indicating expected YYYYMMDDHHMM.
	def test_parse_time_stamp_invalid_length_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("20260224120")

		self.assertIn("Expected YYYYMMDDHHMM format", str(ctx.exception))

	# Test that Time Stamp rejects a 10-digit value.
	# Input: "2602241200" (10 chars).
	# Expected: ValueError indicating expected YYYYMMDDHHMM.
	def test_parse_time_stamp_10_digit_value_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("2602241200")

		self.assertIn("Expected YYYYMMDDHHMM format", str(ctx.exception))

	# Test that invalid calendar dates are rejected.
	# Input: "202602311200" (Feb 31, invalid).
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_invalid_calendar_date_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("202602311200")

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that invalid hour is rejected.
	# Input: "202602242400" (hour 24 invalid).
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_invalid_hour_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("202602242400")

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that invalid minute is rejected.
	# Input: "202602241260" (minute 60 invalid).
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_invalid_minute_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("202602241260")

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that non-numeric YYYYMMDDHHMM is rejected.
	# Input: "20260224AA00".
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_non_numeric_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("20260224AA00")

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that future YYYYMMDDHHMM values are rejected.
	# Input: (current year + 1) + "01010000".
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_future_datetime_raises_value_error(self):
		future_year = pm._REFERENCE_YEAR + 1
		value = f"{future_year}01010000"

		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp(value)

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that CC-prefixed Time Stamp values are rejected by this parser.
	# Input: "T12602241200".
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_time_stamp_cc_prefixed_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_time_stamp("T12602241200")

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 9: Purpose (parse_purpose)
	# -------------------------------------------------------------------------

	# Test that Purpose accepts valid CMEP values case-insensitively.
	# Input: "ok" in lowercase.
	# Expected: The original value is accepted and returned without raising.
	def test_parse_purpose_valid_value_is_accepted_case_insensitive(self):
		result = parse_purpose("ok")
		self.assertEqual(result, "ok")

	# Test that Purpose rejects values outside the CMEP allowed set.
	# Input: "INVALID_PURPOSE".
	# Expected: ValueError mentioning the illegal value and the allowed purposes list.
	def test_parse_purpose_invalid_value_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_purpose("INVALID_PURPOSE")

		self.assertIn("illegal value", str(ctx.exception))
		self.assertIn("Purposes allowed are", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 10: Commodity (parse_commodity)
	# -------------------------------------------------------------------------

	# Test that Commodity accepts supported utility codes case-insensitively.
	# Input: "E", "G", "W", "S" and lowercase variants.
	# Expected: Returned unchanged (original casing preserved).
	def test_parse_commodity_allows_supported_codes_case_insensitive(self):
		self.assertEqual(parse_commodity("E"), "E")
		self.assertEqual(parse_commodity("G"), "G")
		self.assertEqual(parse_commodity("W"), "W")
		self.assertEqual(parse_commodity("S"), "S")

		self.assertEqual(parse_commodity("e"), "e")
		self.assertEqual(parse_commodity("g"), "g")
		self.assertEqual(parse_commodity("w"), "w")
		self.assertEqual(parse_commodity("s"), "s")

	# Test that Commodity rejects unsupported codes.
	# Input: "X".
	# Expected: ValueError mentioning allowed values.
	def test_parse_commodity_rejects_invalid_value(self):
		with self.assertRaises(ValueError) as ctx:
			parse_commodity("X")

		self.assertIn("Allowed values are", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 11: Units (parse_units)
	# -------------------------------------------------------------------------

	# Test that a valid Units value parses successfully.
	# Input: "GAL".
	# Expected: Accepted and returned unchanged (raw input preserved).
	def test_parse_units_valid_value_parses(self):
		result = parse_units("GAL", "W")
		self.assertEqual(result, "GAL")

	# Test that a valid register-style unit parses successfully.
	# Input: "PULSEREG".
	# Expected: Accepted and returned unchanged (raw input preserved).
	def test_parse_units_valid_reg_variant_parses(self):
		result = parse_units("PULSEREG", "E")
		self.assertEqual(result, "PULSEREG")

	# Test that Units rejects blank string.
	# Input: "".
	# Expected: ValueError indicating Units cannot be blank.
	def test_parse_units_blank_string_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_units("", "W")

		self.assertIn("Units value cannot be blank", str(ctx.exception))

	# Test that Units rejects whitespace-only string.
	# Input: "   ".
	# Expected: ValueError indicating Units cannot be blank.
	def test_parse_units_whitespace_string_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_units("   ", "W")

		self.assertIn("Units value cannot be blank", str(ctx.exception))

	# Test that Units rejects invalid type.
	# Input: None.
	# Expected: ValueError indicating illegal type.
	def test_parse_units_invalid_type_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_units(None, "W")

		self.assertIn("Column Units contains an illegal type", str(ctx.exception))

	# Test that illegal unit values are rejected.
	# Input: "$".
	# Expected: ValueError indicating an illegal Units value.
	def test_parse_units_invalid_value_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_units("$", "W")

		self.assertIn("Column Units contains an illegal value", str(ctx.exception))

	# Test that Units cannot be used for the wrong commodity.
	# Input: "KWH" with commodity "W".
	# Expected: ValueError indicating an illegal Units value.
	def test_parse_units_wrong_commodity_is_rejected(self):
		print("parse_units file:", inspect.getsourcefile(parse_units))
		print("parse_units signature:", str(inspect.signature(parse_units)))

		with self.assertRaises(ValueError) as ctx:
			parse_units("KWH", "W")

		self.assertIn("Column Units contains an illegal value", str(ctx.exception))



	# -------------------------------------------------------------------------
	# Column 12: Calculation Constant (parse_calculation_constant)
	# -------------------------------------------------------------------------

	# Test that Calculation Constant returns None for None.
	# Input: None.
	# Expected: None.
	def test_parse_calculation_constant_none_returns_none(self):
		self.assertIsNone(parse_calculation_constant(None))

	# Test that Calculation Constant returns None for empty string.
	# Input: "".
	# Expected: None.
	def test_parse_calculation_constant_empty_string_returns_none(self):
		self.assertIsNone(parse_calculation_constant(""))

	# Test that Calculation Constant returns None for whitespace-only string.
	# Input: "   ".
	# Expected: None.
	def test_parse_calculation_constant_whitespace_string_returns_none(self):
		self.assertIsNone(parse_calculation_constant("   "))

	# Test that Calculation Constant accepts ints and floats (returns float for ints).
	# Input: 3 and 1.5.
	# Expected: 3.0 and 1.5.
	def test_parse_calculation_constant_numeric_values_are_accepted(self):
		self.assertEqual(parse_calculation_constant(3), 3.0)
		self.assertEqual(parse_calculation_constant(1.5), 1.5)

	# Test that Calculation Constant accepts numeric strings.
	# Input: "2.25".
	# Expected: Returned as float 2.25.
	def test_parse_calculation_constant_numeric_string_is_accepted(self):
		self.assertEqual(parse_calculation_constant("2.25"), 2.25)

	# Test that Calculation Constant rejects non-numeric non-empty values.
	# Input: "NOPE".
	# Expected: ValueError indicating invalid Calculation Constant.
	def test_parse_calculation_constant_invalid_value_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_calculation_constant("NOPE")

		self.assertIn("Calculation Constant is invalid", str(ctx.exception))

	# Test that Calculation Constant rejects NaN values.
	# Input: "NaN".
	# Expected: ValueError indicating Calculation Constant must be a finite float.
	def test_parse_calculation_constant_nan_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_calculation_constant("NaN")

		self.assertIn("Calculation Constant is invalid", str(ctx.exception))
		self.assertIn("finite floating point number", str(ctx.exception))

	# Test that Calculation Constant rejects infinite values.
	# Input: "inf" and "-inf".
	# Expected: ValueError indicating Calculation Constant must be a finite float.
	def test_parse_calculation_constant_infinite_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_calculation_constant("inf")

		self.assertIn("Calculation Constant is invalid", str(ctx.exception))
		self.assertIn("finite floating point number", str(ctx.exception))

		with self.assertRaises(ValueError) as ctx:
			parse_calculation_constant("-inf")

		self.assertIn("Calculation Constant is invalid", str(ctx.exception))
		self.assertIn("finite floating point number", str(ctx.exception))


	# -------------------------------------------------------------------------
	# Column 13: Interval (parse_interval)
	# -------------------------------------------------------------------------

	# Test that Interval returns None when blank (interval step not supplied).
	# Input: "".
	# Expected: None.
	def test_parse_interval_blank_string_returns_none(self):
		self.assertIsNone(parse_interval(""))

	# Test that Interval returns None when whitespace-only (interval step not supplied).
	# Input: "   ".
	# Expected: None.
	def test_parse_interval_whitespace_string_returns_none(self):
		self.assertIsNone(parse_interval("   "))

	# Test that Interval returns None when None is provided (interval step not supplied).
	# Input: None.
	# Expected: None.
	def test_parse_interval_none_returns_none(self):
		self.assertIsNone(parse_interval(None))

	# Test that a valid MMDDHHMM Interval duration parses successfully.
	# Input: "00000015" (every 15 minutes).
	# Expected: Structure with format "MMDDHHMM" and delta fields.
	def test_parse_interval_valid_mmddhhmm_duration_parses(self):
		result = parse_interval("00000015")

		self.assertEqual(result["raw"], "00000015")
		self.assertEqual(result["format"], "MMDDHHMM")
		self.assertIn("delta", result)

		delta = result["delta"]
		self.assertEqual(delta["months"], 0)
		self.assertEqual(delta["days"], 0)
		self.assertEqual(delta["hours"], 0)
		self.assertEqual(delta["minutes"], 15)

	# Test that Interval normalizes minute overflow into hours.
	# Input: "00000090" (90 minutes).
	# Expected: 1 hour, 30 minutes.
	def test_parse_interval_normalizes_minutes_overflow(self):
		result = parse_interval("00000090")
		delta = result["delta"]

		self.assertEqual(delta["months"], 0)
		self.assertEqual(delta["days"], 0)
		self.assertEqual(delta["hours"], 1)
		self.assertEqual(delta["minutes"], 30)

	# Test that Interval normalizes hour overflow into days.
	# Input: "00002500" (25 hours).
	# Expected: 1 day, 1 hour.
	def test_parse_interval_normalizes_hours_overflow(self):
		result = parse_interval("00002500")
		delta = result["delta"]

		self.assertEqual(delta["months"], 0)
		self.assertEqual(delta["days"], 1)
		self.assertEqual(delta["hours"], 1)
		self.assertEqual(delta["minutes"], 0)

	# Test that Interval rejects 12-digit digit values (not a duration encoding).
	# Input: "202602230000".
	# Expected: ValueError indicating invalid Interval duration.
	def test_parse_interval_12_digit_value_is_rejected(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval("202602230000")

		self.assertIn("Invalid Interval", str(ctx.exception))
		self.assertIn("12-digit value", str(ctx.exception))

	# Test that Interval rejects non-digit MMDDHHMM duration values.
	# Input: "0000AA15".
	# Expected: ValueError indicating invalid Interval.
	def test_parse_interval_non_numeric_mmddhhmm_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval("0000AA15")

		self.assertIn("Invalid Interval", str(ctx.exception))

	# Test that Interval rejects invalid length values.
	# Input: "00015" (5 chars).
	# Expected: ValueError indicating invalid Interval.
	def test_parse_interval_invalid_length_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval("00015")

		self.assertIn("Invalid Interval", str(ctx.exception))

	# Test that Interval rejects a 0-length duration.
	# Input: "00000000".
	# Expected: ValueError indicating interval delta cannot be 0 length.
	def test_parse_interval_zero_length_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval("00000000")

		self.assertIn("Interval delta cannot be 0 length", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 14: Count (parse_count)
	# -------------------------------------------------------------------------

	# Test that Count accepts integer values.
	# Input: 0 and 5.
	# Expected: Returned unchanged.
	def test_parse_count_accepts_integer_values(self):
		self.assertEqual(parse_count(0), 0)
		self.assertEqual(parse_count(5), 5)

	# Test that Count accepts numeric strings and converts them to integers.
	# Input: "5" and " 10 ".
	# Expected: Returned as integers 5 and 10.
	def test_parse_count_string_digit_is_converted_to_int(self):
		self.assertEqual(parse_count("5"), 5)
		self.assertEqual(parse_count(" 10 "), 10)

	# Test that Count rejects blank string.
	# Input: "".
	# Expected: ValueError indicating Count is required and must be numeric.
	def test_parse_count_blank_string_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_count("")

		self.assertIn("Invalid 'Count'", str(ctx.exception))

	# Test that Count rejects None.
	# Input: None.
	# Expected: ValueError indicating Count is required and must be numeric.
	def test_parse_count_none_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_count(None)

		self.assertIn("Invalid 'Count'", str(ctx.exception))

	# Test that Count rejects non-numeric string values.
	# Input: "abc".
	# Expected: ValueError indicating Count is required and must be numeric.
	def test_parse_count_non_numeric_string_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_count("abc")

		self.assertIn("Invalid 'Count'", str(ctx.exception))

	# Test that Count rejects float values.
	# Input: 1.5.
	# Expected: ValueError indicating Count is required and must be numeric.
	def test_parse_count_float_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_count(1.5)

		self.assertIn("Invalid 'Count'", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Column 15+: Data (parse_interval_data)
	# -------------------------------------------------------------------------

	def test_parse_record_interval_column_present_computes_missing_interval_datetime(self):
		# If the Interval column is present, later interval datetimes may be blank and will be computed.
		row = self._get_valid_row()
		row[12] = "00000100"
		row[17] = ""

		try:
			parse_record(row, row_number=11)
		except Exception as exc:
			self.fail(f"Valid record row with Interval column present raised an unexpected exception: {exc}")

	# Test that parse_interval_data accepts valid rows when Interval is not supplied and returns list-of-lists.
	# Input: Two full intervals: [dt, protocol, usage] x2, interval=None.
	# Expected: No exception, output is list-of-lists, usage values converted to floats.
	def test_parse_interval_data_valid_two_intervals_without_interval_returns_list_of_lists(self):
		potential_interval_data = [
			"200101010000", "r", "1.25",
			"200101010015", "", "0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=None,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["200101010000", "r", 1.25],
				["200101010015", "", 0.0],
			],
		)

	# Test that parse_interval_data rejects a 10-digit datetime for the first interval.
	# Input: "0101010000" (10 digits, not supported), interval=None, count=1.
	# Expected: ValueError indicating the first interval datetime must be YYYYMMDDHHMM.
	def test_parse_interval_data_rejects_10_digit_datetime_for_first_interval(self):
		potential_interval_data = [
			"0101010000", "R", "1.0",
		]

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				potential_interval_data,
				interval=None,
				count=1,
			)

		self.assertIn("First interval datetime must be a valid YYYYMMDDHHMM datetime", str(ctx.exception))
		self.assertIn("Received: 0101010000", str(ctx.exception))

	# Test that parse_interval_data computes later datetimes when Interval step is supplied and datetime is blank.
	# Input: Interval step 15 minutes, 2 intervals, second datetime blank.
	# Expected: Second datetime computed as +15 minutes.
	def test_parse_interval_data_with_interval_computes_missing_datetimes(self):
		interval = parse_interval("00000015")
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"", "R", "2.0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=interval,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["200101010000", "R", 1.0],
				["200101010015", "R", 2.0],
			],
		)

	# Test that parse_interval_data treats whitespace datetime as blank when Interval step is supplied.
	# Input: second datetime is "   ".
	# Expected: second datetime computed.
	def test_parse_interval_data_with_interval_treats_whitespace_datetime_as_blank(self):
		interval = parse_interval("00000015")
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"   ", "R", "2.0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=interval,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["200101010000", "R", 1.0],
				["200101010015", "R", 2.0],
			],
		)

	# Test that parse_interval_data allows provided datetimes when Interval step is supplied if they match computed values.
	# Input: Interval step 15 minutes, 2 intervals, second datetime provided and correct.
	# Expected: Successful parse.
	def test_parse_interval_data_with_interval_allows_matching_provided_datetime(self):
		interval = parse_interval("00000015")
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"200101010015", "R", "2.0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=interval,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["200101010000", "R", 1.0],
				["200101010015", "R", 2.0],
			],
		)

	# Test that parse_interval_data rejects provided datetimes when Interval step is supplied if they do not match computed values.
	# Input: Interval step 15 minutes, 2 intervals, second datetime provided but incorrect.
	# Expected: ValueError indicating datetime mismatch.
	def test_parse_interval_data_with_interval_datetime_mismatch_raises_value_error(self):
		interval = parse_interval("00000015")
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"200101010030", "R", "2.0",
		]

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				potential_interval_data,
				interval=interval,
				count=2,
			)

		self.assertIn("Interval datetime mismatch", str(ctx.exception))

	# Test that parse_interval_data respects interval normalization during computation (minutes overflow).
	# Input: Interval step 75 minutes, 2 intervals, second datetime blank.
	# Expected: Second datetime computed as +1:15.
	def test_parse_interval_data_with_interval_uses_normalized_delta(self):
		interval = parse_interval("00000075")
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"", "R", "2.0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=interval,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["200101010000", "R", 1.0],
				["200101010115", "R", 2.0],
			],
		)

	# Test that monthly interval clamping works (Jan 31 + 1 month -> Feb 28 in non-leap year).
	# Input: Interval step 1 month, start 202101310000, second datetime blank.
	# Expected: Second datetime computed as 202102280000.
	def test_parse_interval_data_with_monthly_interval_clamps_end_of_month(self):
		interval = parse_interval("01000000")
		potential_interval_data = [
			"202101310000", "R", "1.0",
			"", "R", "2.0",
		]

		result = parse_interval_data(
			potential_interval_data,
			interval=interval,
			count=2,
		)

		self.assertEqual(
			result,
			[
				["202101310000", "R", 1.0],
				["202102280000", "R", 2.0],
			],
		)

	# Test that parse_interval_data allows protocol to be None.
	# Input: Protocol None.
	# Expected: Successful parse with protocol preserved as None.
	def test_parse_interval_data_allows_none_protocol(self):
		result = parse_interval_data(
			["200101010000", None, "1.0"],
			interval=None,
			count=1,
		)

		self.assertEqual(result, [["200101010000", None, 1.0]])

	# Test that parse_interval_data rejects Count <= 0.
	# Input: count=0.
	# Expected: ValueError indicating invalid Count value.
	def test_parse_interval_data_count_zero_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "1.0"],
				interval=None,
				count=0,
			)

		self.assertIn("Invalid 'Count' value", str(ctx.exception))

	# Test that parse_interval_data rejects a Data column whose index count is not divisible by 3.
	# Input: 4 elements (not 3-per-interval).
	# Expected: ValueError indicating invalid count of indexes.
	def test_parse_interval_data_invalid_index_count_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "1.0", "EXTRA"],
				interval=None,
				count=1,
			)

		self.assertIn("Invalid count of indexes in 'Data' column", str(ctx.exception))

	# Test that parse_interval_data rejects when Count does not match the provided number of sets (too large).
	# Input: One interval (3 elements), but Count is 2.
	# Expected: ValueError indicating mismatch.
	def test_parse_interval_data_count_mismatch_too_large_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "1.0"],
				interval=None,
				count=2,
			)

		self.assertIn("must equal the value of 'Count'", str(ctx.exception))

	# Test that parse_interval_data rejects when Count does not match the provided number of sets (too small).
	# Input: Two intervals (6 elements), but Count is 1.
	# Expected: ValueError indicating mismatch.
	def test_parse_interval_data_count_mismatch_too_small_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				[
					"200101010000", "R", "1.0",
					"200101010015", "R", "2.0",
				],
				interval=None,
				count=1,
			)

		self.assertIn("must equal the value of 'Count'", str(ctx.exception))

	# Test that parse_interval_data rejects when the first data record is blank.
	# Input: First element is "".
	# Expected: ValueError indicating the first data record must be the datetime of the first interval.
	def test_parse_interval_data_first_record_blank_datetime_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["", "R", "1.0"],
				interval=None,
				count=1,
			)

		self.assertIn("first data record must be the datetime of the first interval", str(ctx.exception))

	# Test that parse_interval_data rejects when the first data record is not a valid datetime.
	# Input: "200113010000" (month 13 invalid).
	# Expected: ValueError indicating the first data record must be the datetime of the first interval.
	def test_parse_interval_data_first_record_invalid_datetime_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200113010000", "R", "1.0"],
				interval=None,
				count=1,
			)

		self.assertIn("First interval datetime must be a valid YYYYMMDDHHMM datetime", str(ctx.exception))
		self.assertIn("Received: 200113010000", str(ctx.exception))

	# Test that parse_interval_data rejects invalid Protocol Text values.
	# Input: Protocol is "Z".
	# Expected: ValueError with allowed values listed.
	def test_parse_interval_data_invalid_protocol_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "Z", "1.0"],
				interval=None,
				count=1,
			)

		self.assertIn("Received 'Protocol Text' value of: Z", str(ctx.exception))
		self.assertIn("Allowed values are '', 'E', 'A', 'N', and 'R'", str(ctx.exception))

	# Test that parse_interval_data rejects non-numeric Usage values.
	# Input: Usage is "abc".
	# Expected: ValueError indicating conversion failure.
	def test_parse_interval_data_invalid_usage_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "abc"],
				interval=None,
				count=1,
			)

		self.assertIn("Received interval usage value of: abc", str(ctx.exception))
		self.assertIn("unable to be converted to a floating point number", str(ctx.exception))

	# Test that parse_interval_data rejects missing datetimes for non-first intervals when Interval is not supplied.
	# Input: interval=None, second datetime blank.
	# Expected: ValueError indicating every interval must include a datetime.
	def test_parse_interval_data_without_interval_missing_datetime_raises_value_error(self):
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"", "R", "2.0",
		]

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				potential_interval_data,
				interval=None,
				count=2,
			)

		self.assertIn("When the 'Interval' field is not supplied", str(ctx.exception))

	# Test that parse_interval_data rejects invalid non-first datetimes when Interval is not supplied.
	# Input: interval=None, second datetime invalid.
	# Expected: ValueError indicating required datetime is invalid.
	def test_parse_interval_data_invalid_non_first_datetime_raises_value_error(self):
		potential_interval_data = [
			"200101010000", "R", "1.0",
			"200113010000", "R", "2.0",
		]

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				potential_interval_data,
				interval=None,
				count=2,
			)

		self.assertIn("must be a valid YYYYMMDDHHMM datetime", str(ctx.exception))

	# Test that parse_interval_data rejects when computed next datetime would be in the future.
	# Input: Interval step 2 minutes, first datetime set so computed second datetime is in the future.
	# Expected: ValueError indicating computed datetime is in the future.
	def test_parse_interval_data_computed_future_datetime_raises_value_error(self):
		now = pm._REFERENCE_NOW
		start_dt = now - datetime.timedelta(minutes=1)
		start_str = start_dt.strftime("%Y%m%d%H%M")

		interval = parse_interval("00000002")
		potential_interval_data = [
			start_str, "R", "1.0",
			"", "R", "2.0",
		]

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				potential_interval_data,
				interval=interval,
				count=2,
			)

		self.assertIn("Computed next interval datetime is in the future", str(ctx.exception))

	# Test that parse_interval_data rejects Count None.
	# Input: count=None.
	# Expected: ValueError indicating invalid Count value.
	def test_parse_interval_data_count_none_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "1.0"],
				interval=None,
				count=None,
			)

		self.assertIn("Invalid 'Count' value", str(ctx.exception))

	# Test that parse_interval_data rejects negative Count values.
	# Input: count=-1.
	# Expected: ValueError indicating invalid Count value.
	def test_parse_interval_data_count_negative_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "1.0"],
				interval=None,
				count=-1,
			)

		self.assertIn("Invalid 'Count' value", str(ctx.exception))

	# Test that parse_interval_data rejects NaN usage values.
	# Input: Usage is "NaN".
	# Expected: ValueError indicating usage must be a finite float.
	def test_parse_interval_data_nan_usage_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "NaN"],
				interval=None,
				count=1,
			)

		self.assertIn("Interval usage must be a finite floating point number", str(ctx.exception))

	# Test that parse_interval_data rejects infinite usage values.
	# Input: Usage is "inf" and "-inf".
	# Expected: ValueError indicating usage must be a finite float.
	def test_parse_interval_data_infinite_usage_raises_value_error(self):
		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "inf"],
				interval=None,
				count=1,
			)

		self.assertIn("Interval usage must be a finite floating point number", str(ctx.exception))

		with self.assertRaises(ValueError) as ctx:
			parse_interval_data(
				["200101010000", "R", "-inf"],
				interval=None,
				count=1,
			)

		self.assertIn("Interval usage must be a finite floating point number", str(ctx.exception))

	# -------------------------------------------------------------------------
	# Non-column-specific: parse_record (integration for a single row)
	# -------------------------------------------------------------------------

	# Test that a fully valid record row passes parse_record without raising.
	# Input: A valid row with at least 17 columns (this fixture represents Count=2, so it includes 2 interval sets).
	# Expected: No exception raised.
	def test_parse_record_valid_row_parses_without_error(self):
		row = self._get_valid_row()

		try:
			parse_record(row, row_number=1)
		except Exception as exc:
			self.fail(f"Valid record row raised an unexpected exception: {exc}")

	# Test that parse_record returns the correct number of output rows and output column count.
	# Input: A valid row with Count=2 and Interval step present.
	# Expected: Two output rows, each with 17 columns, and computed second datetime.
	def test_parse_record_valid_row_returns_expected_rows(self):
		row = self._get_valid_row()
		result = parse_record(row, row_number=1)

		self.assertEqual(len(result), 2)
		self.assertEqual(len(result[0]), 17)
		self.assertEqual(len(result[1]), 17)

		self.assertEqual(result[0][14], "202602251200")
		self.assertEqual(result[1][14], "202602251300")

		self.assertEqual(result[0][15], "R")
		self.assertEqual(result[1][15], "R")
		self.assertEqual(result[0][16], 25.0)
		self.assertEqual(result[1][16], 10.0)

	# Test that parse_record enforces MEPMD01 record type and includes row context.
	# Input: A valid row with element 0 changed, row_number=7.
	# Expected: ValueError mentioning expected MEPMD01 and row 7.
	def test_parse_record_invalid_record_type_raises_value_error(self):
		row = self._get_valid_row()
		row[0] = "NOT-MEPMD01"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=7)

		self.assertIn("Expected: MEPMD01", str(ctx.exception))
		self.assertIn("row 7", str(ctx.exception))

	# Test that parse_record rejects rows with fewer than 17 columns.
	# Input: A valid row truncated to 16 elements, row_number=3.
	# Expected: IndexError mentioning fewer than 17 columns and row 3.
	def test_parse_record_short_row_raises_index_error(self):
		row = self._get_valid_row()[0:16]

		with self.assertRaises(IndexError) as ctx:
			parse_record(row, row_number=3)

		self.assertIn("fewer than 17 columns", str(ctx.exception))
		self.assertIn("row 3", str(ctx.exception))

	# Test that parse_record fails when Record Version fails validation.
	# Input: CC-prefixed Record Version.
	# Expected: ValueError indicating invalid Record Version.
	def test_parse_record_invalid_record_version_raises_value_error(self):
		row = self._get_valid_row()
		row[1] = "T1260224"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=4)

		self.assertIn("Invalid Record Version", str(ctx.exception))

	# Test that parse_record fails when Time Stamp is in the future.
	# Input: A valid row with Time Stamp set to future YYYYMMDDHHMM.
	# Expected: ValueError indicating invalid Time Stamp.
	def test_parse_record_future_time_stamp_raises_value_error(self):
		row = self._get_valid_row()

		future_year = pm._REFERENCE_YEAR + 1
		row[6] = f"{future_year}01010000"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=2)

		self.assertIn("Invalid Time Stamp", str(ctx.exception))

	# Test that parse_record validates Purpose using column 9 (index 8).
	# Input: A valid row with Purpose changed to an invalid value.
	# Expected: ValueError indicating Column Purpose contains an illegal value.
	def test_parse_record_invalid_purpose_raises_value_error(self):
		row = self._get_valid_row()
		row[8] = "NOPE"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=5)

		self.assertIn("Column Purpose contains an illegal value", str(ctx.exception))

	# Test that parse_record fails when Commodity is invalid.
	# Input: A valid row with Commodity changed to "X".
	# Expected: ValueError indicating invalid Commodity.
	def test_parse_record_invalid_commodity_raises_value_error(self):
		row = self._get_valid_row()
		row[9] = "X"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=6)

		self.assertIn("Allowed values are", str(ctx.exception))

	# Test that parse_record fails when Units is invalid.
	# Input: A valid row with Units changed to "$".
	# Expected: ValueError indicating illegal Units value.
	def test_parse_record_invalid_units_raises_value_error(self):
		row = self._get_valid_row()
		row[10] = "$"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=7)

		self.assertIn("Column Units contains an illegal value", str(ctx.exception))

	# Test that parse_record fails when Calculation Constant is invalid.
	# Input: A valid row with Calculation Constant set to "NOPE".
	# Expected: ValueError indicating invalid Calculation Constant.
	def test_parse_record_invalid_calculation_constant_raises_value_error(self):
		row = self._get_valid_row()
		row[11] = "NOPE"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=7)

		self.assertIn("Calculation Constant is invalid", str(ctx.exception))

	# Test that parse_record fails when Interval is invalid (12-digit digit value is not a duration encoding).
	# Input: A valid row with Interval set to "202602230000".
	# Expected: ValueError indicating invalid Interval.
	def test_parse_record_invalid_interval_raises_value_error(self):
		row = self._get_valid_row()
		row[12] = "202602230000"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=3)

		self.assertIn("Invalid Interval", str(ctx.exception))

	# Test that parse_record fails when Count is invalid.
	# Input: A valid row with Count set to non-numeric string.
	# Expected: ValueError indicating invalid Count.
	def test_parse_record_invalid_count_raises_value_error(self):
		row = self._get_valid_row()
		row[13] = "NOT_A_NUMBER"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=8)

		self.assertIn("Invalid 'Count'", str(ctx.exception))

	# Test that parse_record fails when Count is 0 (parse_interval_data requires Count > 0).
	# Input: A valid row with Count set to "0".
	# Expected: ValueError indicating invalid Count value for interval data.
	def test_parse_record_count_zero_raises_value_error_from_interval_data(self):
		row = self._get_valid_row()
		row[13] = "0"

		with self.assertRaises(ValueError) as ctx:
			parse_record(row, row_number=9)

		self.assertIn("Invalid 'Count' value", str(ctx.exception))

	def test_parse_record_blank_interval_column_parses_when_all_interval_datetimes_present(self):
		# If the Interval column is blank, every interval must include a full YYYYMMDDHHMM datetime.
		row = self._get_valid_row()
		row[12] = ""
		row[17] = "202602251300"

		try:
			parse_record(row, row_number=10)
		except Exception as exc:
			self.fail(f"Valid record row with blank Interval column raised an unexpected exception: {exc}")

	# -------------------------------------------------------------------------
	# Non-column-specific: parse_cmep_file (file-level integration)
	# -------------------------------------------------------------------------

	# Test integration path for a valid CMEP file with one valid row.
	# Input: A CSV file containing one valid MEPMD01 row.
	# Expected: No exception raised.
	def test_parse_cmep_file_valid_file_parses_without_error(self):
		cmep_file = self._write_temp_csv("mepmd01_valid.csv", self._get_valid_row_csv_line())

		try:
			parse_cmep_file(str(cmep_file), "")
		except Exception as exc:
			self.fail(f"Valid CMEP file raised an unexpected exception: {exc}")

	# Test that parse_cmep_file writes the expected output file and row count.
	# Input: A valid CMEP file with one record containing two intervals.
	# Expected: Output file created next to input with two output rows.
	def test_parse_cmep_file_writes_expected_output_file(self):
		cmep_file = self._write_temp_csv("mepmd01_valid_output.csv", self._get_valid_row_csv_line())
		parse_cmep_file(str(cmep_file), "")

		output_file = cmep_file.with_name(f"{cmep_file.stem}_parsed.csv")
		self.assertTrue(output_file.is_file())

		with open(output_file, "r", newline="", encoding="utf-8") as handle:
			lines_out = [line for line in handle.read().splitlines() if line.strip() != ""]

		self.assertEqual(len(lines_out), 2)

	# Test that a truly empty file is rejected as empty.
	# Input: File with 0 bytes.
	# Expected: ValueError containing "CMEP file is empty".
	def test_parse_cmep_file_empty_file_raises_value_error(self):
		cmep_file = self._write_temp_csv("mepmd01_empty.csv", "")

		with self.assertRaises(ValueError) as ctx:
			parse_cmep_file(str(cmep_file), "")

		self.assertIn("CMEP file is empty", str(ctx.exception))

	# Test that a blank first row is treated as malformed, not valid.
	# Input: File with an empty line before valid data.
	# Expected: ValueError containing "First row is blank".
	def test_parse_cmep_file_blank_first_row_raises_value_error(self):
		cmep_file = self._write_temp_csv(
			"mepmd01_blank_first_row.csv",
			"\n" + self._get_valid_row_csv_line(),
		)

		with self.assertRaises(ValueError) as ctx:
			parse_cmep_file(str(cmep_file), "")

		self.assertIn("First row is blank", str(ctx.exception))

	# Test that a short first row is rejected.
	# Input: File whose first row has fewer than 17 columns.
	# Expected: IndexError mentioning fewer than 17 columns in the first row.
	def test_parse_cmep_file_short_first_row_raises_index_error(self):
		short_row = self._get_valid_row()[0:16]
		cmep_file = self._write_temp_csv(
			"mepmd01_short_first_row.csv",
			",".join(short_row) + "\n",
		)

		with self.assertRaises(IndexError) as ctx:
			parse_cmep_file(str(cmep_file), "")

		self.assertIn("fewer than 17 columns in the first row", str(ctx.exception))

	# Test that parse_cmep_file reports short rows beyond the first row with the correct row number.
	# Input: File with a valid first row and short second row.
	# Expected: IndexError mentioning fewer than 17 columns and row 2.
	def test_parse_cmep_file_short_second_row_raises_index_error_with_row_number(self):
		short_row = self._get_valid_row()[0:16]
		cmep_file = self._write_temp_csv(
			"mepmd01_short_second_row.csv",
			self._get_valid_row_csv_line() + ",".join(short_row) + "\n",
		)

		with self.assertRaises(IndexError) as ctx:
			parse_cmep_file(str(cmep_file), "")

		self.assertIn("fewer than 17 columns", str(ctx.exception))
		self.assertIn("row 2", str(ctx.exception))

	# Test that parse_cmep_file fails when record_type is invalid in the first row.
	# Input: File with first row record_type set to "MEPMD02".
	# Expected: ValueError mentioning expected MEPMD01.
	def test_parse_cmep_file_invalid_record_type_raises_value_error(self):
		row = self._get_valid_row()
		row[0] = "MEPMD02"

		cmep_file = self._write_temp_csv(
			"mepmd01_invalid_record_type.csv",
			",".join(row) + "\n",
		)

		with self.assertRaises(ValueError) as ctx:
			parse_cmep_file(str(cmep_file), "")

		self.assertIn("Expected: MEPMD01", str(ctx.exception))

	# Test that parse_cmep_file raises FileNotFoundError for a missing input file path.
	# Input: Non-existent file path.
	# Expected: FileNotFoundError raised by open(...).
	def test_parse_cmep_file_nonexistent_path_raises_file_not_found_error(self):
		with self.assertRaises(FileNotFoundError):
			parse_cmep_file("/tmp/this_file_should_not_exist_mepmd01.csv", "")

	# Test that parse_cmep_file does not leave partial output or temp output on failure after writing begins.
	# Input: File with a valid first row and an invalid second row.
	# Expected: parse_cmep_file raises, and neither the final output nor temp output exists.
	def test_parse_cmep_file_failure_does_not_leave_partial_or_temp_output(self):
		valid_line = self._get_valid_row_csv_line()

		row = self._get_valid_row()
		row[0] = "MEPMD02"
		invalid_line = ",".join(row) + "\n"

		cmep_file = self._write_temp_csv(
			"mepmd01_partial_failure.csv",
			valid_line + invalid_line,
		)

		output_file = cmep_file.with_name(f"{cmep_file.stem}_parsed.csv")
		temp_output_file = Path(str(output_file) + ".tmp")

		with self.assertRaises(ValueError):
			parse_cmep_file(str(cmep_file), "")

		self.assertFalse(output_file.exists())
		self.assertFalse(temp_output_file.exists())

	# Test that parse_cmep_file does not leave partial output or temp output on commit failure.
	# Input: A valid CMEP file, but the atomic replace fails.
	# Expected: parse_cmep_file raises, and neither the final output nor temp output exists.
	def test_parse_cmep_file_commit_failure_does_not_leave_partial_or_temp_output(self):
		cmep_file = self._write_temp_csv("mepmd01_commit_fail.csv", self._get_valid_row_csv_line())

		output_file = cmep_file.with_name(f"{cmep_file.stem}_parsed.csv")
		temp_output_file = Path(str(output_file) + ".tmp")

		with mock.patch("parse_mepmd01.os.replace", side_effect=OSError("replace failed")):
			with self.assertRaises(OSError):
				parse_cmep_file(str(cmep_file), "")

		self.assertFalse(output_file.exists())
		self.assertFalse(temp_output_file.exists())

if __name__ == "__main__":
	unittest.main()
