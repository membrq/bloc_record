module BlocRecord
   class Collection < Array

     def update_all(updates)
       ids = self.map(&:id)

       self.any? ? self.first.class.update(ids, updates) : false
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

     #everyone whose name is not John
     def not(*args)
       array = []
       for item in self
         for key in args.keys
           array += self.first.class.where(:name => !args.value)
         end
       end
       array
     end

   end
 end
