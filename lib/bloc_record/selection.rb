require 'sqlite3'

 module Selection

   COLUMNS = [:name, :phone_number]

   def find(*ids)
     if ids.length == 1
       find_one(ids.first)
     else
       rows = connection.execute <<-SQL
         SELECT #{columns.join ","} FROM #{table}
         WHERE id IN (#{ids.join(",")});
       SQL

       rows_to_array(rows)
     end
   end

   def find_one(id)
     row = connection.get_first_row <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       WHERE id = #{id};
     SQL

     init_object_from_row(row)
   end

   def find_by(attribute, value)
     row = connection.get_first_row <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
     SQL

     init_object_from_row(row)
   end

   #method missing
   def method_missing(method, *args, &block)
     if COLUMNS != nil
       COLUMNS.find do |column|
         puts "This is the column: #{column}"
         match = column
         if match === method
           puts "This method exists"
           find_by(match, args[0])
         end
       end
     else
       super
       puts "There's no method called #{method}"
     end
   end

   #find_each; default batch size = 1000
   def find_each(start: 2000, batch_size: 2000) #(query = {})
     rows = connection.execute <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       LIMIT #{batch_size]};
     SQL

     for row in rows_to_array(rows)
       yield(row)
     end
   end

   #find_in_batches
   def find_in_batches(start: 4000, batch_size: 2000)
     rows = connection.execute <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       LIMIT #{batch_size};
     SQL

     yield(rows_to_arrays(rows))
     #yield(rows_to_array(rows.slice(start, (start + batch_size)))
   end

   def take(num=1)
     if num > 1
       rows = connection.execute <<-SQL
         SELECT #{columns.join ","} FROM #{table}
         ORDER BY random()
         LIMIT #{num};
       SQL

       rows_to_array(rows)
     else
       take_one
     end
   end

   def take_one
     row = connection.get_first_row <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       ORDER BY random()
       LIMIT 1;
     SQL

     init_object_from_row(row)
   end

   def first
     row = connection.get_first_row <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       ORDER BY id ASC LIMIT 1;
     SQL

     init_object_from_row(row)
   end

   def last
     row = connection.get_first_row <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       ORDER BY id DESC LIMIT 1;
     SQL

     init_object_from_row(row)
   end

   def all
     rows = connection.execute <<-SQL
       SELECT #{columns.join ","} FROM #{table};
     SQL

     rows_to_array(rows)
   end

   def where(*args)
     if args.count > 1
       expression = args.shift
       params = args
     else
       case args.first
       when String
         expression = args.first
       when Hash
         expression_hash = BlocRecord::Utility.convert_keys(args.first)
         expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
       end
     end

     sql = <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       WHERE #{expression};
     SQL

     rows = connection.execute(sql, params)
     rows_to_array(rows)
   end

   #
   # Caller should pass in names of columns to order by,
   # and sorting direction in the format:
   # Entry.order("columnA DIRECTION", "columnB DIRECTION")
   def order(*args)
       if args.count > 1
         order_and_direction = args.join(",")
       else
         order_and_direction = args.first.to_s
       end

     rows = connection.execute <<-SQL
       SELECT * FROM #{table}
       ORDER BY #{order_and_direction};
     SQL
     rows_to_array(rows)
   end

   def join(*args)
     if args.count > 1
       joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
       rows = connection.execute <<-SQL
         SELECT * FROM #{table} #{joins}
       SQL
     else
       case args.first
       when String
         rows = connection.execute <<-SQL
           SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
         SQL
       when Symbol
         rows = connection.execute <<-SQL
           SELECT * FROM #{table}
           INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
         SQL
       #when Hash
        # rows = connection.execute <<-SQL
        #   SELECT * FROM #{table}
        #   INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        # SQL
       end
     end
     rows_to_array(rows)
   end

   #add method for 2 inner joins; for Hogwarts tables
   #args will be column names (args[0], args[1], etc)
   #for exercise, column names have been hard-coded in SQL code
   def two_inner_joins(*args)
     if args.count > 0
       rows = connection.execute <<-SQL
       SELECT * FROM #{professor}
       INNER JOIN #{department} ON #{professor}.#{department}_id = #{department}.id
       INNER JOIN #{compensation} ON #{professor}.id = #{compensation}.#{professor}_id
       SQL
     end
     rows_to_arrays(rows)
   end

   private

   def init_object_from_row(row)
     if row
       data = Hash[columns.zip(row)]
       new(data)
     end
   end

   def rows_to_array(rows)
     collection = BlocRecord::Collection.new
     rows.each { |row| collection << new(Hash[columns.zip(row)]) }
     collection
   end

   #def rows_to_arrays_sub_array(rows)
     # array.slice(start, length)
    # rows.map { |row| new(Hash[columns.zip(row)]) }
   #end
 end
