from __future__ import annotations
from typing import Any, Dict, List, Optional, Union

import calendar
import csv
import datetime
import optparse
import os


# Purpose:
#	Processes California Metering Exchange Protocol (CMEP) MEPMD01 files (as CSV) and validates format, then outputs a symmetrical file, with one interval per row.
# Notes:
#	Functions starting with an underscore are "helper" functions for the main parsing functions:
# 		(ie. _function_name(...), _validate_protocol(...), _resolve_output_file_name(...), etc.)
# 	Order of parsing is in column order, and runs in the function parse_record(...)
#		parse_record(...) processes one row in the CMEP file at a time, but expands the records to the number in the 'Count' column, if it's a valid row
#	
# MEPMD01 Format (note, CMEP files do not contain column header):
# Col Num	Col Name				Col Type											Col Destription
# 1			Record Type				String												Always 'MEPMD01'
# 2			Record Version			Date ("YYYYMMDD")									"19970819" by default
# 3			Sender ID				String(16)											ID for the entity sending this record (usually sender company name)
# 4			Sender Customer ID		String(12)											ID for the Sender of the file (assigned by Meter Vendor)
# 5			Receiver ID				String(16)											ID for the entity Receipient of the file (eg. VertexOne, Sensus Analytics, etc)
# 6			Receiver Customer ID	String(12)											ID for the Receiver of the file (assigned by Meter Vendor)
# 7			Time Stamp				Datetime(YYYYMMDDHHMM)								Date and Time of record creation
# 8			Meter ID				String(12)											ID of physical meter.  This is usually some arbitrary combination of letters and numbers that make up a meter manufacturer serial number.
# 9			Purpose					ENUM												Reason for Data Transmission (see parse_purpose(...))
# 10		Commodity				ENUM												Type of service for this record (see parse_commodity(...))
# 11		Units					ENUM												Unit of Measure for this record (see parse_units(...))
# 12		Calculation Constant	Float												Optional muliplier value to be applied to the usage in this record
# 13		Interval				Datetime(MMDDHHMM)									Describes the time interval between readings (eg. 00000100 = hourly, 00000015 = every 15 minutes, 01000000 = monthly)
# 14		Count					Integer												Number of intervals in this record (eg. if the record contains 1 interval of water usage, this is 1.  If it contains 4 intervals of electric usage, this is 4)
# 15+		Data					list((Datetime(YYYYMMDDHHMM), ENUM, Float), ...)	Sets of 3 values for each interval in format: [Datetime of Interval, Protocol Text, and Interval Usage].  If 'Interval' column's value is not supplied, each set requires the datetime of the interval.  If it is present, the datetime is only required for the first interval.  (See parse_interval_data(...) and related functions)
#	
# Details of CMEP formats can be found at: https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf
#	Note: In CMEP spec, one may see date formats such as CCYYMMDD.  This is the same as YYYYMMDD.  CC is the century of the date.  
#		Generally, this will always be the 2000s, unless loading significant historical data as if it were interval data.
#	
# Author: Nick Medovich <nick.medovich@vertexone.ai>

# Helper variables (don't touch, please)
_OUTPUT_WRITERS = {}

_REFERENCE_NOW = None
_REFERENCE_TODAY = None
_REFERENCE_YEAR = None

# Type Inference Variables
RecordVersionParseResult = Dict[str, Union[str, None, datetime.date]]
TimeStampParseResult = Dict[str, Union[str, None, datetime.datetime]]
IntervalDelta = Dict[str, int]
IntervalParseResult = Dict[str, Union[str, IntervalDelta]]
IntervalDataRow = List[Union[str, None, float]]
ParsedRow = List[Union[str, int, float, None]]

# -------------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------------

def _ensure_reference_time() -> None:
	"""Freeze a single reference time for the full run.

	The parser needs one consistent "current time" so future-datetime checks do not change mid-file and tests remain deterministic.

	Returns:
		None
	"""
	global _REFERENCE_NOW, _REFERENCE_TODAY, _REFERENCE_YEAR

	if _REFERENCE_NOW is None:
		_REFERENCE_NOW = datetime.datetime.now()
		_REFERENCE_TODAY = _REFERENCE_NOW.date()
		_REFERENCE_YEAR = _REFERENCE_TODAY.year

def _get_reference_now() -> datetime.datetime:
	"""Return the frozen reference "now" datetime used for this run.

	Returns:
		datetime.datetime: Frozen reference "now".
	"""
	_ensure_reference_time()
	return _REFERENCE_NOW

def _get_reference_today() -> datetime.date:
	"""Return the frozen reference "today" date used for this run.

	Returns:
		datetime.date: Frozen reference "today".
	"""
	_ensure_reference_time()
	return _REFERENCE_TODAY

def _get_current_year() -> int:
	"""Return the frozen reference year used for Record Version validation.

	Record Version YY/YYYY validation is only allowed through the current year.

	Returns:
		int: Frozen reference year.
	"""
	_ensure_reference_time()
	return _REFERENCE_YEAR

def _validate_record_version_input(potential_record_version: str, row_number: Optional[int] = None) -> str:
	"""Validate Record Version input invariants for later strict parsing.

	Record Version drives strict date parsing later, so this enforces:
	- value must be a string
	- trimmed value must be fixed-width YYYYMMDD (8 chars)

	Args:
		potential_record_version (str): Raw Record Version value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		str: Trimmed Record Version, exactly 8 characters.

	Raises:
		ValueError: If type is not string, or if trimmed value is not length 8.
	"""
	row_context = _format_row_context(row_number)

	if not isinstance(potential_record_version, str):
		raise ValueError(f"Invalid Record Version type{row_context}.  Expected string.  Received: {type(potential_record_version)}")

	value = potential_record_version.strip()
	if len(value) != 8:
		raise ValueError(f"Invalid Record Version{row_context}.  Expected YYYYMMDD format.  Received: {potential_record_version}")

	return value

def _try_parse_yyyymmdd(value: str, current_year: int) -> Optional[RecordVersionParseResult]:
	"""Try to parse a YYYYMMDD date string, returning None if invalid.

	Validation rules:
	- Must be all digits
	- Must be a real calendar date
	- Year must be <= current_year
	- Date must not be after reference "today"

	Args:
		value (str): Candidate YYYYMMDD string.
		current_year (int): Upper bound for year validation.

	Returns:
		Optional[RecordVersionParseResult]:
			Dict when valid with keys: raw, format, version_number, release_date.
			None when invalid.
	"""
	if not value.isdigit():
		return None

	yyyy = int(value[0:4])
	month = int(value[4:6])
	day = int(value[6:8])

	if yyyy > current_year:
		return None

	try:
		release_date = datetime.date(yyyy, month, day)
	except ValueError:
		return None

	if release_date > _get_reference_today():
		return None

	return {
		"raw": value,
		"format": "YYYYMMDD",
		"version_number": None,
		"release_date": release_date,
	}

def _validate_time_stamp_input(potential_time_stamp: str, row_number: Optional[int] = None) -> str:
	"""Validate Time Stamp input invariants for later strict parsing.

	Time Stamp parsing assumes fixed-width YYYYMMDDHHMM, so this enforces:
	- value must be a string
	- trimmed value must be exactly 12 characters

	Args:
		potential_time_stamp (str): Raw Time Stamp value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		str: Trimmed Time Stamp, exactly 12 characters.

	Raises:
		ValueError: If type is not string, or if trimmed value is not length 12.
	"""
	row_context = _format_row_context(row_number)

	if not isinstance(potential_time_stamp, str):
		raise ValueError(f"Invalid Time Stamp type{row_context}.  Expected string.  Received: {type(potential_time_stamp)}")

	value = potential_time_stamp.strip()
	if len(value) != 12:
		raise ValueError(f"Invalid Time Stamp{row_context}.  Expected YYYYMMDDHHMM format.  Received: {potential_time_stamp}")

	return value

def _try_parse_hour(potential_hour: str) -> Optional[int]:
	"""Try to parse an hour component (00-23), returning None if invalid.

	Args:
		potential_hour (str): Two-digit hour value.

	Returns:
		Optional[int]: Parsed hour or None when invalid.
	"""
	if not potential_hour.isdigit():
		return None

	hour = int(potential_hour)
	if not (0 <= hour <= 23):
		return None

	return hour

def _try_parse_minute(potential_minute: str) -> Optional[int]:
	"""Try to parse a minute component (00-59), returning None if invalid.

	Args:
		potential_minute (str): Two-digit minute value.

	Returns:
		Optional[int]: Parsed minute or None when invalid.
	"""
	if not potential_minute.isdigit():
		return None

	minute = int(potential_minute)
	if not (0 <= minute <= 59):
		return None

	return minute

def _try_parse_yyyymmddhhmm(value: Optional[object], current_year: Optional[int] = None) -> Optional[TimeStampParseResult]:
	"""Try to parse a YYYYMMDDHHMM timestamp, returning None if invalid.

	Validation rules:
	- None input returns None
	- Non-strings are stringified defensively
	- Must be exactly 12 digits
	- Must be a real calendar datetime
	- Must not be after reference "now"

	Args:
		value (Optional[object]): Candidate timestamp value.
		current_year (Optional[int]): Upper bound for year validation. Defaults to the reference year.

	Returns:
		Optional[TimeStampParseResult]:
			Dict when valid with keys: raw, format, version_number, time_stamp.
			None when invalid.
	"""
	# Allow legacy callers that do not pass current_year (ex: parse_data).
	# Keep the parameter for existing call sites that do pass it.
	if current_year is None:
		current_year = _get_current_year()

	if value is None:
		return None

	if not isinstance(value, str):
		value = str(value)

	value = value.strip()

	if len(value) != 12:
		return None

	if not value.isdigit():
		return None

	yyyy = int(value[0:4])
	month = int(value[4:6])
	day = int(value[6:8])

	hour = _try_parse_hour(value[8:10])
	if hour is None:
		return None

	minute = _try_parse_minute(value[10:12])
	if minute is None:
		return None

	try:
		time_stamp = datetime.datetime(yyyy, month, day, hour, minute)
	except ValueError:
		return None

	if time_stamp > _get_reference_now():
		return None

	return {
		"raw": value,
		"format": "YYYYMMDDHHMM",
		"version_number": None,
		"time_stamp": time_stamp,
	}

def _validate_protocol(potential_protocol: Optional[object], row_number: Optional[int] = None) -> Optional[str]:
	"""Validate Protocol Text (Protocol) field.

	Allowed values:
	- "" (empty) = OK and validated
	- "E" = estimated
	- "A" = adjusted
	- "N" = empty/no entry
	- "R" = raw/unvalidated

	Args:
		potential_protocol (Optional[object]): Raw protocol value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		Optional[str]: Original protocol value (as provided) when valid, or None when input is None.

	Raises:
		ValueError: If the value is not one of the allowed protocol values.
	"""
	row_context = _format_row_context(row_number)

	if potential_protocol is None:
		return None

	if not isinstance(potential_protocol, str):
		potential_protocol = str(potential_protocol)

	value = potential_protocol.strip()
	if value.upper() not in ["", "E", "A", "N", "R"]:
		raise ValueError(
			f"Received 'Protocol Text' value of: {potential_protocol}{row_context}. Allowed values are '', 'E', 'A', 'N', and 'R'"
		)
	return potential_protocol

def _parse_required_interval_datetime(potential_datetime: str, context: str, row_number: Optional[int] = None) -> datetime.datetime:
	"""Parse and require a valid YYYYMMDDHHMM datetime.

	Args:
		potential_datetime (str): Candidate datetime value.
		context (str): Error context prefix.
		row_number (Optional[int]): Row number for error context.

	Returns:
		datetime.datetime: Parsed datetime.

	Raises:
		ValueError: If parsing fails.
	"""
	parsed = _try_parse_yyyymmddhhmm(potential_datetime)
	if parsed is None:
		row_context = ""
		if row_number is not None:
			row_context = f" in row {row_number}"
		raise ValueError(f"{context} must be a valid YYYYMMDDHHMM datetime. Received: {potential_datetime}{row_context}")
	return parsed["time_stamp"]

def _format_dt_yyyymmddhhmm(dt_value: datetime.datetime) -> str:
	"""Format datetime as YYYYMMDDHHMM."""
	return dt_value.strftime("%Y%m%d%H%M")

def _is_blank(value: Optional[object]) -> bool:
	"""Return True if value is None or a blank/whitespace-only string."""
	return value is None or (isinstance(value, str) and value.strip() == "")

def _normalize_interval_delta(delta: Dict[str, Union[int, str]], row_number: Optional[int] = None) -> IntervalDelta:
	"""Normalize an interval delta mapping into non-negative integer components.

	Converts values to integers, rejects negatives, carries minutes into hours and hours into days.

	Args:
		delta (Dict[str, Union[int, str]]): Mapping with optional keys months, days, hours, minutes.
		row_number (Optional[int]): Row number for error context.

	Returns:
		IntervalDelta: Dict with keys months, days, hours, minutes as non-negative ints.

	Raises:
		ValueError: If any component is negative.
	"""
	row_context = _format_row_context(row_number)

	months = int(delta.get("months", 0))
	days = int(delta.get("days", 0))
	hours = int(delta.get("hours", 0))
	minutes = int(delta.get("minutes", 0))

	if months < 0 or days < 0 or hours < 0 or minutes < 0:
		raise ValueError(f"Interval delta cannot contain negative values{row_context}. Received: {delta}")

	if minutes >= 60:
		hours += minutes // 60
		minutes = minutes % 60

	if hours >= 24:
		days += hours // 24
		hours = hours % 24

	return {"months": months, "days": days, "hours": hours, "minutes": minutes}

def _parse_interval_delta_mmddhhmm(value: str, row_number: Optional[int] = None) -> IntervalParseResult:
	"""Parse an Interval duration encoding MMDDHHMM into a normalized delta dict.

	Format: MMDDHHMM (8 digits)
	Examples:
	- 00000100 = hourly
	- 00000015 = every 15 minutes
	- 01000000 = monthly (1 month)

	Args:
		value (str): Raw interval duration encoding.
		row_number (Optional[int]): Row number for error context.

	Returns:
		IntervalParseResult: Dict with keys raw, format, delta.

	Raises:
		ValueError: If the value is not a string, not 8 digits, or encodes a zero-length interval.
	"""
	row_context = _format_row_context(row_number)

	if not isinstance(value, str):
		raise ValueError(f"Invalid Interval type{row_context}. Expected string. Received: {type(value)}")

	raw = value.strip()
	if not raw.isdigit() or len(raw) != 8:
		raise ValueError(
			f"Invalid Interval{row_context}. Expected duration encoding of MMDDHHMM (8 digits). Received: {value}"
		)

	months = int(raw[0:2])
	days = int(raw[2:4])
	hours = int(raw[4:6])
	minutes = int(raw[6:8])

	delta = _normalize_interval_delta(
		{"months": months, "days": days, "hours": hours, "minutes": minutes},
		row_number=row_number,
	)

	if delta["months"] == 0 and delta["days"] == 0 and delta["hours"] == 0 and delta["minutes"] == 0:
		raise ValueError(f"Invalid Interval. Interval delta cannot be 0 length{row_context}. Received: {value}")

	return {"raw": raw, "format": "MMDDHHMM", "delta": delta}

def _get_interval_delta(interval: Optional[IntervalParseResult], row_number: Optional[int] = None) -> Optional[IntervalDelta]:
	"""Extract and normalize the delta from an interval object returned by parse_interval.

	Args:
		interval (Optional[IntervalParseResult]): Parsed interval object or None.
		row_number (Optional[int]): Row number for error context.

	Returns:
		Optional[IntervalDelta]: Normalized delta dict, or None if interval is None.

	Raises:
		ValueError: If the interval object is malformed.
	"""
	row_context = _format_row_context(row_number)

	if interval is None:
		return None
	if not isinstance(interval, dict):
		raise ValueError(f"Invalid interval object type{row_context}. Expected dict. Received: {type(interval)}")

	delta = interval.get("delta")
	if not isinstance(delta, dict):
		raise ValueError(f"Interval object is missing required 'delta' dictionary{row_context}.")
	return _normalize_interval_delta(delta, row_number=row_number)

def _add_months(dt_value: datetime.datetime, months: int) -> datetime.datetime:
	"""Add month offsets to a datetime, clamping day to last day of target month.

	Args:
		dt_value (datetime.datetime): Base datetime.
		months (int): Number of months to add.

	Returns:
		datetime.datetime: Shifted datetime.
	"""
	if months == 0:
		return dt_value

	month_index = (dt_value.month - 1) + months
	new_year = dt_value.year + (month_index // 12)
	new_month = (month_index % 12) + 1

	last_day = calendar.monthrange(new_year, new_month)[1]
	new_day = min(dt_value.day, last_day)

	return dt_value.replace(year=new_year, month=new_month, day=new_day)

def _add_interval_delta(previous_dt: datetime.datetime, delta: IntervalDelta, row_number: Optional[int] = None) -> datetime.datetime:
	"""Compute the next interval datetime using a normalized interval delta.

	Adds months via calendar month stepping, then adds days, hours, and minutes via timedelta.
	Fails if the computed datetime would be in the future relative to the reference "now".

	Args:
		previous_dt (datetime.datetime): Previous interval datetime.
		delta (IntervalDelta): Normalized delta dict with months, days, hours, minutes.
		row_number (Optional[int]): Row number for error context.

	Returns:
		datetime.datetime: Next interval datetime.

	Raises:
		ValueError: If the computed next datetime is in the future.
	"""
	row_context = _format_row_context(row_number)

	months = delta["months"]
	days = delta["days"]
	hours = delta["hours"]
	minutes = delta["minutes"]

	next_dt = _add_months(previous_dt, months)
	next_dt = next_dt + datetime.timedelta(days=days, hours=hours, minutes=minutes)

	if next_dt > _get_reference_now():
		raise ValueError(
			f"Computed next interval datetime is in the future{row_context}: {next_dt.strftime('%Y%m%d%H%M')}"
		)

	return next_dt

def _validate_usage(potential_usage: str, row_number: Optional[int] = None) -> float:
	"""Validate and convert interval usage to a finite float.

	Args:
		potential_usage (str): Raw usage value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		float: Validated usage value.

	Raises:
		ValueError: If conversion fails, or if value is NaN, +inf, or -inf.
	"""
	row_context = _format_row_context(row_number)

	try:
		value = float(potential_usage)
	except Exception as err:
		raise ValueError(f"Received interval usage value of: {potential_usage}{row_context}.  This was unable to be converted to a floating point number.  Error: {err}")

	if value != value or value == float("inf") or value == float("-inf"):
		raise ValueError(f"Received interval usage value of: {potential_usage}{row_context}.  Interval usage must be a finite floating point number.")

	return value

def _crash_out_output_file(temp_output_file_name: Optional[str]) -> None:
	"""Best-effort cleanup for a failed run.

	Closes any open writer for the temp file, then removes the temp file if it exists.

	Args:
		temp_output_file_name (Optional[str]): Temporary output file path.

	Returns:
		None
	"""
	try:
		_close_output_writer(temp_output_file_name)
	except Exception:
		pass

	try:
		if temp_output_file_name and os.path.exists(temp_output_file_name):
			os.remove(temp_output_file_name)
	except Exception:
		pass

def _resolve_temp_output_file_name(output_file_name: str) -> str:
	"""Return the temp output file name used for atomic write semantics."""
	return f"{output_file_name}.tmp"

def _commit_temp_output_file(temp_output_file_name: str, output_file_name: str) -> None:
	"""Atomically replace final output with temp output on the same filesystem."""
	os.replace(temp_output_file_name, output_file_name)

def _resolve_output_file_name(input_file_name: str, output_file_name: Optional[str]) -> str:
	"""Resolve the final output path for the parsed CSV.

	If output_file_name is provided:
	- use it as-is
	- if it has no directory component, place it next to the input file

	If output_file_name is blank/None:
	- derive "<input_stem>_parsed.csv" next to the input file

	Args:
		input_file_name (str): Input CMEP file path.
		output_file_name (Optional[str]): User-supplied output path (blank treated as not provided).

	Returns:
		str: Resolved output file path.
	"""
	if output_file_name is not None and str(output_file_name).strip() != "":
		output_file_name = str(output_file_name).strip()
		output_dir = os.path.dirname(output_file_name)
		if output_dir == "":
			input_dir = os.path.dirname(input_file_name)
			if input_dir != "":
				return os.path.join(input_dir, output_file_name)
		return output_file_name

	input_dir = os.path.dirname(input_file_name)
	stem = os.path.splitext(os.path.basename(input_file_name))[0]

	derived_name = f"{stem}_parsed.csv"

	if input_dir != "":
		return os.path.join(input_dir, derived_name)
	return derived_name

def _get_output_writer(output_file_name: str) -> Any:
	"""Get or create a cached csv.writer for the given output path.

	Keeps a single csv.writer open per output path and creates the directory once
	to avoid reopening the file on every write (closed on fail).

	Args:
		output_file_name (str): Output file path.

	Returns:
		Any: csv.writer instance for the given output path.
	"""
	if output_file_name not in _OUTPUT_WRITERS:
		out_dir = os.path.dirname(output_file_name)
		if out_dir != "":
			os.makedirs(out_dir, exist_ok=True)

		output_handle = open(output_file_name, "w", newline="", encoding="utf-8")
		writer = csv.writer(output_handle)
		_OUTPUT_WRITERS[output_file_name] = (output_handle, writer)

	return _OUTPUT_WRITERS[output_file_name][1]

def _close_output_writer(output_file_name: str) -> None:
	"""Close and remove the cached output writer for the given path."""
	writer_entry = _OUTPUT_WRITERS.pop(output_file_name, None)
	if writer_entry is None:
		return
	output_handle, _ = writer_entry
	output_handle.close()

def _format_row_context(row_number: Optional[int]) -> str:
	"""Format row number context for error messages."""
	if row_number is None:
		return ""
	return f" in row {row_number}"

# -------------------------------------------------------------------------
# Record parsing functions
# -------------------------------------------------------------------------

def parse_record_type(potential_record_type: str, row_number: Optional[int]) -> str:
	"""Validate and return the Record Type for MEPMD01.

	The only valid Record Type for MEPMD01 files is "MEPMD01" (case-insensitive).

	Args:
		potential_record_type (str): Raw Record Type value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		str: Original Record Type value when valid.

	Raises:
		ValueError: If type is not string or value is not MEPMD01.
	"""
	if not isinstance(potential_record_type, str):
		if row_number is not None:
			raise ValueError(
				f"Invalid Record Type type in row {row_number}.  Expected string.  Received: {type(potential_record_type)}"
			)
		raise ValueError(f"Invalid Record Type type.  Expected string.  Received: {type(potential_record_type)}")

	if potential_record_type.lower() != "mepmd01":
		if row_number is not None:
			raise ValueError(f"Received Record Type of: {potential_record_type} in row {row_number}.  Expected: MEPMD01")
		raise ValueError(f"Received Record Type of: {potential_record_type}.  Expected: MEPMD01")

	return potential_record_type

def parse_record_version(potential_record_version: str, row_number: Optional[int] = None) -> RecordVersionParseResult:
	"""Parse and validate Record Version as YYYYMMDD.

	Args:
		potential_record_version (str): Raw Record Version value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		RecordVersionParseResult: Dict with keys raw, format, version_number, release_date.

	Raises:
		ValueError: If Record Version is invalid.
	"""
	row_context = _format_row_context(row_number)

	current_year = _get_current_year()
	potential_date = _validate_record_version_input(potential_record_version, row_number=row_number)

	yyyy_result = _try_parse_yyyymmdd(potential_date, current_year)
	if yyyy_result is not None:
		return yyyy_result

	raise ValueError(f"Invalid Record Version.  Expected YYYYMMDD format.  Received: {potential_record_version}{row_context}")

def parse_time_stamp(potential_time_stamp: str, row_number: Optional[int] = None) -> TimeStampParseResult:
	"""Parse and validate Time Stamp as YYYYMMDDHHMM.

	Args:
		potential_time_stamp (str): Raw Time Stamp value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		TimeStampParseResult: Dict with keys raw, format, version_number, time_stamp.

	Raises:
		ValueError: If Time Stamp is invalid.
	"""
	row_context = _format_row_context(row_number)

	current_year = _get_current_year()
	potential_date_time = _validate_time_stamp_input(potential_time_stamp, row_number=row_number)

	yyyy_result = _try_parse_yyyymmddhhmm(potential_date_time, current_year)
	if yyyy_result is not None:
		return yyyy_result

	raise ValueError(f"Invalid Time Stamp.  Expected YYYYMMDDHHMM format.  Received: {potential_time_stamp}{row_context}")

def parse_purpose(potential_purpose: str, row_number: Optional[int] = None) -> str:
	"""Validate Purpose field value.

	Allowed values:
	- OK
	- RESEND
	- SUMMARY
	- HISTORY
	- PROFILE
	- TEMPLATE

	Args:
		potential_purpose (str): Raw Purpose value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		str: Original Purpose value when valid.

	Raises:
		ValueError: If Purpose is not one of the allowed values.
	"""
	row_context = _format_row_context(row_number)

	valid_purposes = ["OK", "RESEND", "SUMMARY", "HISTORY", "PROFILE", "TEMPLATE"]
	if potential_purpose.upper() in valid_purposes:
		return potential_purpose
	raise ValueError(f"Column Purpose contains an illegal value: {potential_purpose}{row_context}.\nPurposes allowed are: OK, RESEND, SUMMARY, HISTORY, PROFILE, and TEMPLATE.")

def parse_commodity(potential_commodity: str, row_number: Optional[int] = None) -> str:
	"""Validate Commodity field value.

	Allowed values:
	- E (Electric)
	- G (Gas)
	- W (Water)
	- S (Steam)

	Args:
		potential_commodity (str): Raw Commodity value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		str: Original Commodity value when valid.

	Raises:
		ValueError: If Commodity is not one of the allowed values.
	"""
	row_context = _format_row_context(row_number)

	if potential_commodity.upper() not in ["E", "G", "W", "S"]:
		raise ValueError(f"Receive Commodity of {potential_commodity}{row_context}.  Allowed values are: 'E' (electric), 'G' (gas), 'W' (water), and 'S' (steam).")
	return potential_commodity

def parse_units(potential_units: str, commodity: str, row_number: Optional[int] = None) -> str:
    """Validate Units against a commodity-specific allowlist.

    Units must match the commodity to prevent cross-service mistakes. Allows "<UNIT>REG" variants
    that appear in real exports.

    Args:
        potential_units (str): Raw Units value (string, not blank after trimming).
        commodity (str): Raw Commodity value (string, not blank after trimming).
        row_number (Optional[int]): Row number for error context.

    Returns:
        str: Original Units value when valid.

    Raises:
        ValueError: If any validation fails (type, blank, commodity invalid, units invalid).
    """
    row_context = _format_row_context(row_number)

    if not isinstance(potential_units, str):
        raise ValueError(
            f"Column Units contains an illegal type: {type(potential_units)}{row_context}.  Expected string."
        )

    if not isinstance(commodity, str):
        raise ValueError(
            f"Column Commodity contains an illegal type: {type(commodity)}{row_context}.  Expected string."
        )

    commodity_value = commodity.strip().upper()
    if commodity_value == "":
        raise ValueError(
            f"Column Commodity contains an illegal value: {commodity}{row_context}.\nCommodity value cannot be blank."
        )

    value = potential_units.strip().upper()
    if value == "":
        raise ValueError(
            f"Column Units contains an illegal value: {potential_units}{row_context}.\nUnits value cannot be blank."
        )

    base_units = {
        "W": [
            "GAL",
            "KGAL",
            "MGAL",
            "L",
            "KL",
            "ML",
            "CF",
            "CCF",
            "MCF",
            "FT3",
            "CUFT",
            "M3",
            "CUM",
            "ACFT",
            "USGAL",
            "IMPGAL",
        ],
        "E": [
            "PULSE",
            "COUNT",
            "PULSES",
            "IMP",
            "IMPULSE",
            "W",
            "KW",
            "MW",
            "GW",
            "GKW",
            "WH",
            "KWH",
            "MWH",
            "GWH",
            "GKWH",
            "VA",
            "KVA",
            "MVA",
            "GVA",
            "GKVA",
            "VAR",
            "KVAR",
            "MVAR",
            "GVAR",
            "VAH",
            "KVAH",
            "MVAH",
            "GVAH",
            "VARH",
            "KVARH",
            "MVARH",
            "GVARH",
            "GKVARH",
        ],
        "G": [
            "THERM",
            "THERMS",
            "DTH",
            "BTU",
            "MMBTU",
            "CCF",
            "MCF",
            "CF",
            "FT3",
            "CUFT",
            "M3",
            "CUM",
            "SCF",
            "MSCF",
        ],
        "S": [
            "LBS",
            "KLB",
            "TON",
            "BTU",
            "MMBTU",
        ],
    }

    if commodity_value not in base_units:
        raise ValueError(
            f"Column Commodity contains an illegal value: {commodity}{row_context}.\n"
            "Commodity allowed values are 'W', 'E', 'G', and 'S'."
        )

    commodity_units = base_units[commodity_value]
    valid_units = set(commodity_units)
    for unit in commodity_units:
        valid_units.add(f"{unit}REG")

    if value in valid_units:
        return potential_units

    raise ValueError(
        f"Column Units contains an illegal value: {potential_units}{row_context}.\n"
        "Units allowed are standard utility usage units for the given commodity, plus sane '<UNIT>REG' variants."
    )

def parse_calculation_constant(potential_calculation_constant: Optional[object], row_number: Optional[int] = None) -> Optional[float]:
	"""Validate and parse Calculation Constant as an optional finite float.

	Blank/None values return None.

	Args:
		potential_calculation_constant (Optional[object]): Raw Calculation Constant value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		Optional[float]: Parsed float when present and valid, otherwise None.

	Raises:
		ValueError: If provided value cannot be converted to float or is not finite.
	"""
	row_context = _format_row_context(row_number)

	if potential_calculation_constant is None:
		return None
	if isinstance(potential_calculation_constant, str) and potential_calculation_constant.strip() == "":
		return None

	try:
		value = float(potential_calculation_constant)
	except (TypeError, ValueError):
		raise ValueError(
			f"Calculation Constant is invalid{row_context}. Received {potential_calculation_constant!r} "
			f"(type: {type(potential_calculation_constant).__name__})"
		)

	if value != value or value == float("inf") or value == float("-inf"):
		raise ValueError(
			f"Calculation Constant is invalid{row_context}. Received {potential_calculation_constant!r} "
			f"(type: {type(potential_calculation_constant).__name__}). Value must be a finite floating point number."
		)

	return value

def write_parsed_rows(output_file_name: str, parsed_rows: List[ParsedRow]) -> None:
	"""Write parsed output rows to the output CSV.

	Args:
		output_file_name (str): Output file path.
		parsed_rows (List[ParsedRow]): Rows returned by parse_record(...).

	Returns:
		None
	"""
	writer = _get_output_writer(output_file_name)
	writer.writerows(parsed_rows)

def parse_interval(potential_interval: Optional[object], row_number: Optional[int] = None) -> Optional[IntervalParseResult]:
	"""Parse Interval duration encoding (MMDDHHMM) when provided.

	Interval is a duration encoding, not a calendar datetime.

	Format:
	- MMDDHHMM (8 digits)

	Examples:
	- 00000100 = hourly (0 months, 0 days, 1 hour, 0 minutes)
	- 00000015 = every 15 minutes
	- 01000000 = monthly (1 month)

	Args:
		potential_interval (Optional[object]): Raw Interval value. Blank/None is treated as not provided.
		row_number (Optional[int]): Row number for error context.

	Returns:
		Optional[IntervalParseResult]: Parsed interval object when provided, otherwise None.

	Raises:
		ValueError: If malformed or looks like a 12-digit datetime.
	"""
	row_context = _format_row_context(row_number)

	if _is_blank(potential_interval):
		return None

	value = str(potential_interval).strip()

	if len(value) == 12 and value.isdigit():
		raise ValueError(
			f"Invalid Interval{row_context}. Expected duration encoding (MMDDHHMM), but received 12-digit value: {potential_interval}"
		)

	return _parse_interval_delta_mmddhhmm(value, row_number=row_number)

def parse_count(potential_count: Union[int, str], row_number: Optional[int] = None) -> int:
	"""Parse and validate Count (number of interval triplets).

	Args:
		potential_count (Union[int, str]): Raw Count value.
		row_number (Optional[int]): Row number for error context.

	Returns:
		int: Parsed count.

	Raises:
		ValueError: If missing, blank, non-numeric, or otherwise invalid.
	"""
	row_context = _format_row_context(row_number)

	if potential_count is None:
		raise ValueError(
			f"Invalid 'Count' column{row_context}.  The Count column must be present and contain a number relative to the number of intervals provided in the file."
		)

	if isinstance(potential_count, int):
		return potential_count

	if isinstance(potential_count, str):
		value = potential_count.strip()
		if value == "":
			raise ValueError(
				f"Invalid 'Count' column{row_context}.  The Count column must be present and contain a number relative to the number of intervals provided in the file."
			)
		if value.isdigit():
			return int(value)

	raise ValueError(
		f"Invalid 'Count' column{row_context}.  The Count column must be present and contain a number relative to the number of intervals provided in the file."
	)

def parse_interval_data(potential_interval_data: List[str], interval: Optional[IntervalParseResult], count: int, row_number: Optional[int] = None) -> List[IntervalDataRow]:
	"""Parse and validate Interval Data triplets.

	Interval Data is in groups of 3:
	- [Datetime of Interval, Protocol Text, Interval Usage]

	If Interval is not set, the datetime of each interval is required.
	If Interval is set, only the first interval must include datetime and later datetimes are inferred.

	Args:
		potential_interval_data (List[str]): Flat list starting at column 15, expected length 3 * count.
		interval (Optional[IntervalParseResult]): Parsed Interval object (allows inference when present).
		count (int): Expected number of triplets.
		row_number (Optional[int]): Row number for error context.

	Returns:
		List[IntervalDataRow]: Rows of [YYYYMMDDHHMM (str), protocol (str|None), usage (float)].

	Raises:
		ValueError: On structural issues, invalid datetime/protocol/usage, mismatch with inferred datetimes, or future inferred datetimes.
	"""
	row_context = _format_row_context(row_number)

	if count is None or count <= 0:
		raise ValueError(f"Invalid 'Count' value: {count}. Count must be a positive integer{row_context}.")

	if len(potential_interval_data) % 3 != 0:
		raise ValueError(
			"Invalid count of indexes in 'Data' column. There must be 3 indexes per data element: "
			"Datetime of Interval, Protocol Text, and Usage Amount. See "
			"https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf"
			+ row_context
		)

	number_of_sets = len(potential_interval_data) // 3
	if number_of_sets != count:
		raise ValueError(
			f"Received {number_of_sets} sets of interval data{row_context}. However, the 'Count' value contains {count}. "
			"The number of sets must equal the value of 'Count'."
		)

	delta = _get_interval_delta(interval, row_number=row_number)
	interval_is_present = delta is not None

	first_dt_value = potential_interval_data[0]
	if _is_blank(first_dt_value):
		raise ValueError(
			f"The first data record must be the datetime of the first interval, whether the 'Interval' field contains a record or not{row_context}."
		)

	first_dt = _parse_required_interval_datetime(first_dt_value, "First interval datetime", row_number=row_number)
	first_protocol = _validate_protocol(potential_interval_data[1], row_number=row_number)
	first_usage = _validate_usage(potential_interval_data[2], row_number=row_number)

	interval_sets = []
	interval_sets.append([_format_dt_yyyymmddhhmm(first_dt), first_protocol, first_usage])

	prev_dt = first_dt

	for set_index in range(1, number_of_sets):
		base = set_index * 3
		raw_dt = potential_interval_data[base]
		raw_protocol = potential_interval_data[base + 1]
		raw_usage = potential_interval_data[base + 2]

		protocol = _validate_protocol(raw_protocol, row_number=row_number)
		usage = _validate_usage(raw_usage, row_number=row_number)

		if _is_blank(raw_dt):
			if not interval_is_present:
				raise ValueError(
					f"Interval datetime is missing for interval #{set_index + 1}{row_context}. "
					"When the 'Interval' field is not supplied, every interval must include a full YYYYMMDDHHMM datetime."
				)
			dt_value = _add_interval_delta(prev_dt, delta, row_number=row_number)
		else:
			dt_value = _parse_required_interval_datetime(raw_dt, f"Interval #{set_index + 1} datetime", row_number=row_number)

			if interval_is_present:
				expected = _add_interval_delta(prev_dt, delta, row_number=row_number)
				if dt_value != expected:
					raise ValueError(
						f"Interval datetime mismatch at interval #{set_index + 1}{row_context}. "
						f"Expected {expected.strftime('%Y%m%d%H%M')} based on Interval step, "
						f"but received {dt_value.strftime('%Y%m%d%H%M')}."
					)

		interval_sets.append([_format_dt_yyyymmddhhmm(dt_value), protocol, usage])
		prev_dt = dt_value

	return interval_sets

def parse_record(current_row: List[str], row_number: Optional[int] = None) -> List[ParsedRow]:
	"""Parse one MEPMD01 CSV row into normalized output rows (one per interval).

	Args:
		current_row (List[str]): One CSV row in MEPMD01 column order.
		row_number (Optional[int]): Row number for error context.

	Returns:
		List[ParsedRow]: Output rows, one per interval triplet.

	Raises:
		IndexError: If current_row has fewer than 17 columns.
		ValueError: If any field-level parser rejects a value.
	"""
	if len(current_row) < 17:
		row_context = ""
		if row_number is not None:
			row_context = f" in row {row_number}"
		raise IndexError(
			f"There are fewer than 17 columns{row_context}. Per CMEP format, there are 14 non-usage-data columns, then 3 columns per usage data index.  For data specifications, see: https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf"
		)

	record_type = parse_record_type(current_row[0], row_number)
	record_version = parse_record_version(current_row[1], row_number=row_number)
	sender_id = current_row[2]
	sender_customer_id = current_row[3]
	receiver_id = current_row[4]
	receiver_customer_id = current_row[5]
	time_stamp = parse_time_stamp(current_row[6], row_number=row_number)
	meter_id = current_row[7]
	purpose = parse_purpose(current_row[8], row_number=row_number)
	commodity = parse_commodity(current_row[9], row_number=row_number)
	units = parse_units(current_row[10], commodity, row_number=row_number)
	calculation_constant = parse_calculation_constant(current_row[11], row_number=row_number)
	interval = parse_interval(current_row[12], row_number=row_number)
	count = parse_count(current_row[13], row_number=row_number)
	interval_data = parse_interval_data(current_row[14:], interval, count, row_number=row_number)

	interval_raw = ""
	if interval is not None:
		interval_raw = interval["raw"]

	rows_out = []
	for interval_row in interval_data:
		rows_out.append([
			record_type,
			record_version["raw"],
			sender_id,
			sender_customer_id,
			receiver_id,
			receiver_customer_id,
			time_stamp["raw"],
			meter_id,
			purpose,
			commodity,
			units,
			calculation_constant,
			interval_raw,
			count,
			interval_row[0],
			interval_row[1],
			interval_row[2]
		])
	return rows_out

def parse_cmep_file(cmep_file: str, output_file_name: str) -> None:
	"""Parse a CMEP MEPMD01 CSV file and write normalized output atomically.

	Writes to a temp output file first, then commits the final output file only if all rows parse successfully.

	Args:
		cmep_file (str): Input CMEP CSV file path.
		output_file_name (str): Output CSV file path. Blank triggers default naming next to input.

	Returns:
		None

	Raises:
		FileNotFoundError: If input file does not exist.
		IndexError: If required columns are missing.
		ValueError: If the file is empty, first row is blank, or any row contains invalid values.
	"""
	_ensure_reference_time()

	resolved_output_file_name = output_file_name
	if _is_blank(resolved_output_file_name):
		resolved_output_file_name = _resolve_output_file_name(cmep_file, "")

	temp_output_file_name = _resolve_temp_output_file_name(resolved_output_file_name)
	success = False

	try:
		with open(cmep_file, "r", newline="", encoding="utf-8") as csv_file:
			reader = csv.reader(csv_file)

			first_row = next(reader, None)
			if first_row is None:
				raise ValueError("CMEP file is empty.")
			if not first_row:
				raise ValueError("First row is blank.  CMEP export appears malformed.")

			if len(first_row) < 17:
				raise IndexError(
					"There are fewer than 17 columns in the first row. Per CMEP format, there are 14 non-usage-data columns, then 3 columns per usage data index.  See https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf"
				)

			record_rows = parse_record(first_row, row_number=1)
			write_parsed_rows(temp_output_file_name, record_rows)

			for row_number, row in enumerate(reader, start=2):
				record_rows = parse_record(row, row_number=row_number)
				write_parsed_rows(temp_output_file_name, record_rows)

		success = True
	finally:
		_close_output_writer(temp_output_file_name)

		if success:
			try:
				_commit_temp_output_file(temp_output_file_name, resolved_output_file_name)
				print("Parsing complete!")
			except Exception:
				_crash_out_output_file(temp_output_file_name)
				raise
		else:
			_crash_out_output_file(temp_output_file_name)

def main(cmep_file: str, output_file_name: str = "") -> None:
	"""Entrypoint used by the CLI flow.

	Args:
		cmep_file (str): Input CMEP file path.
		output_file_name (str): Output file path.

	Returns:
		None
	"""
	parse_cmep_file(cmep_file, output_file_name)
	print(f"Successfully wrote {output_file_name}")

if __name__ == "__main__":
	_ensure_reference_time()

	parser = optparse.OptionParser(usage="%prog [OPTIONS] /path/to/cmep/file.csv")
	parser.add_option("-o", "--output-file", default="", dest="output_file_name", help=("Output CSV file path. If not provided, output is written alongside the input as '<input_stem>_parsed.csv'. If provided without a directory, it is written alongside the input."))

	options, args = parser.parse_args()
	if len(args) != 1:
		parser.error("Path to CMEP file is required.  Usage: python3 parse_mepmd01.py [OPTIONS] /path/to/cmep/file.csv")

	input_file_name = args[0]
	if os.path.isfile(input_file_name):
		resolved_output_file_name = _resolve_output_file_name(input_file_name, options.output_file_name)
		main(input_file_name, resolved_output_file_name)
	else:
		raise FileNotFoundError(f"Input CMEP file does not exist: {input_file_name}")