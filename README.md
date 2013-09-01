# Implementing an Importer

The data importer uses a gem called the `voter_gateway`. This piece of code wraps the process of moving data from a flat file (csv format) into a database. Currently this is implemented for postgres. You can find the code for voter_gateway here: https://github.com/3dna/voter_gateway

## Create your importer!

The first step is to create a new file for the importer class to reside in. The class definition is simple

```ruby
module VoterFile
  class TestImport < ImportJob::Base
  end
end
```

Now we have a non-functional importer! congrats!

We can _fake_ the implementation by giving it three methods

```ruby
module VoterFile
  class TestImport < ImportJob::Base
    def initialize(file, state, date)
    end
    
    def connection
    end
    
    def job(driver)
    end
  end
end
```

But it still won't run and will most likely error!

## Getting up and running

Getting it running isn't that hard and we can walk through each of the methods in turn.

### Init

For initialization, we just need to stash the inputs.
```ruby
def initialize(file, state, date)
  @file = file    # Path to file with voter data
  @state = state  # State the voter file corresponds with
  @date = date    # Date when the voter file was compiled
end
```

### Connect!

The connection is more or less a property and for the state importing we want to make sure that the connection is to the correct shard. This 'property' will be used as the basis for the connection the driver (soon to be discussed) will use.

```ruby
def connection
  MasterVoter.on(@state).connection
end
```

### Doing the heavy lifting

The last method is `job` and this is a heavily loaded method that will do everything! It will get passed a driver that actually directs all the data querying and migration. Along the way, you will be issuing commands to the driver which, in turn, will be executed on the servers. We'll cover a few of these commands next. Some things to note:

- The job method takes a parameter that you must accept called the driver.
- Most work done is done on the driver.

#### Loading the file

The first step is to load the file so that the importer can use it.

```ruby
csv = driver.load_file @file do |f|
  f.delimiter = '|' # Specifies the delimiter of the file.
end
```

That's it! Now the object you get returned can be used to do other manipulations!

#### Tell the driver where the target is

The driver doesn't start off knowing where the target is so we need to capture it. To do this we specify the table with a name name and inform it which column uniquely identifies rows.

```ruby
target_table = driver.load_table "#{@state.downcase}_voters" do |t|
  t.set_primary_key 'nbec_guid', 'uuid'
end
```

#### Mapping the file to other names

The next step is optional but is necessary if the columns from the CSV do not match the columns in the target table. This is relatively straight forward:

```ruby
import_data = driver.load_table csv do |t|
  t.default_data_type = :TEXT                                 # The data type to use on a column when not explicit
  t.set_primary_key :county_file_id, :text                    # The column that uniquely identifies a row
  t.add_column "id", type: :INT                               # Adding a column that wasn't there before
  t.map_column "county_file_id", from: "voterid", type: :TEXT # Maps columns
  ...
end
```

The `map_column` is probably the most used (but the first two are required as well). `map_colum` takes the name of the target column as the first parameter and then a bunch of options. Those options are:

| Option | Description |
| --- | --- |
| `:type` | The type of data contained in the column |
| `:as` | A SQL expression that should be used in place of the column name. `$S` signs are replaced by the column name. |
| `:from` | The column to pull from |

You may call `map_column` and `add_column` multiple times.

#### Merging the records together!

Now that we have loaded both tables, we can merge the records. This step has a bunch more optional and required options

```ruby
driver.merge_records do |m|
  m.target_table = target_table                               # Tell the importer where merge into
  m.source_table = import_data                                # Tell the importer where to merge from
  m.exact_match_group :residential_address2                   # Declare how to match on certain columns
  m.fuzzy_match_column :residential_address2
  m.constrain_column :residential_address1, "$S is not null"  # Declare contraints on columns
  m.exclude_column :residential_zip4                          # Exclude columns from source
  m.preserve_column :county_file_id                           # Always move the preserved columns from source to target
  m.merge_column_as :residential_zip5, "$T || $S"             # Apply columns as a SQL expression to resolve merge.
  m.insert_column_as :middle_name, "awkward"                  # Insert a column into target as a SQL expression
  m.return_value_to_source "t.id", :id                        # Re-insert values from target to source
end
```
