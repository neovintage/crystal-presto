# PrestoDB Crystal Driver

prestodb crystal driver

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     presto.cr:
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

Not much to see here now

## Contributing

1. Fork it (<https://github.com/neovintage/presto.cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [rimas silkaitis](https://github.com/neovintage) - creator and maintainer
