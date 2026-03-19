import csv
import os
import optparse


def _resolve_output_file_name(input_file_name, output_file_name):
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

	derived_name = f"{stem}_asymmetrical_rows.csv"

	if input_dir != "":
		return os.path.join(input_dir, derived_name)
	return derived_name

def get_headers(input_file, delimiter, quote_char):
	with open(input_file, newline="") as csv_file:
		reader = csv.reader(
			csv_file,
			delimiter=delimiter,
			quotechar=quote_char
		)
		return next(reader, None)

def get_bad_rows(input_file, delimiter, quote_char, target_length):
	bad_rows = {}
	current_row = 1
	with open(input_file, newline="") as csv_file:
		reader = csv.reader(
			csv_file,
			delimiter=delimiter,
			quotechar=quote_char
		)
		next(reader, None)
		for row in reader:
			current_row += 1
			if len(row) != target_length:
				bad_rows[current_row] = list(row)
	if current_row in bad_rows:
		bad_rows.pop(current_row)
	return bad_rows

def write_bad_rows(output_file, headers, bad_rows, delimiter, quote_char):
	headers_out = ["line_number_from_file"] + list(headers)

	with open(output_file, "w") as csv_file:
		writer_kwargs = {
			"delimiter": delimiter
		}
		if quote_char is not None:
			writer_kwargs["quotechar"] = quote_char

		writer = csv.writer(
			csv_file, 
			delimiter=delimiter,
			quotechar=quote_char
		)
		writer.writerow(headers_out)

		for line_number, row in bad_rows.items():
			writer.writerow([line_number] + list(row))

def main(input_file, output_file, delimiter, quote_char):
	headers = get_headers(input_file, delimiter, quote_char)
	if headers is None:
		raise ValueError(f"Input CSV file is empty: {input_file}")

	bad_rows = get_bad_rows(input_file, delimiter, quote_char, len(headers))
	write_bad_rows(output_file, headers, bad_rows, delimiter, quote_char)

if __name__ == "__main__":
	parser = optparse.OptionParser(usage="%prog [OPTIONS] /path/to/file.csv")
	parser.add_option(
		"-o",
		"--output-file",
		default="",
		dest="output_file_name",
		help=("Output CSV file path. If not provided, output is written alongside the input as '<input_stem>_asymmetrical_rows.csv'. If provided without a directory, it is written alongside the input.")
	)
	parser.add_option(
		"-d",
		"--delim",
		default=",",
		dest="delimiter",
		help=("Delimiter character for the file. Comma (,) by default.")
	)
	parser.add_option(
		"-p",
		"--quote-char",
		default=None,
		dest="quote_char",
		help=("Quote character for the file. Leave empty to disable quoting.")
	)

	options, args = parser.parse_args()
	if len(args) != 1:
		parser.error("Path to CSV file is required. Usage: python3 find_malformed_rows.py [OPTIONS] /path/to/file.csv")

	input_file_name = args[0]
	if os.path.isfile(input_file_name):
		resolved_output_file_name = _resolve_output_file_name(input_file_name, options.output_file_name)
		main(input_file_name, resolved_output_file_name, options.delimiter, options.quote_char)
	else:
		raise FileNotFoundError(f"Input CSV file does not exist: {input_file_name}")