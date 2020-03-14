# PrestoDB Crystal Driver

![CI Status](https://github.com/neovintage/crystal-presto/workflows/Run%20Specs/badge.svg?branch=master)

prestodb crystal driver

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     presto:
       github: neovintage/crystal-presto
   ```

2. Run `shards install`

## Usage

```crystal
require "presto"

DB.open("presto://username:@localhost:8080/tpch/sf1") do |db|
  db.query("select * from customer limit 1") do |q|
    puts q.row_count
    q.each do |rs|
      puts "#{rs.column_name(0)}: #{rs.read}"
    end
  end
end
```

## Development


## Contributors

- [rimas silkaitis](https://github.com/neovintage) - creator and maintainer
