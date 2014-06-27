A = load 'hdfs:///test2' using TextLoader();
COUNTED = FOREACH A GENERATE SIZE($0);
store COUNTED into '$out_path';