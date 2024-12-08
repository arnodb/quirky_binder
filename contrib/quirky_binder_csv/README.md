# Quirky Binder - CSV

`quirky_binder_csv` helps reading and writing CSV files.

## Setup

* add `quirky_binder_csv` to `[build-dependencies]`
* add `csv = "1"` to `[dependencies]`

## Read

```
use quirky_binder_csv::read_csv;

{
  (
      read_csv(
        input_file: "input/hello_universe.csv",
        fields: [("hello", "String"), ("universe", "usize")],
        has_headers: true,
      )
    - ...
  )
}
```

## Write

```
use quirky_binder_csv::write_csv;

{
  (
    ...
    - write_csv(
        output_file: "output/hello_universe.csv",
        has_headers: true,
      )
  )
}
```

