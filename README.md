## Analyze

## Compile

        sbt assembly

The result jar will be located under ./target/scala-2.10/analyze-assembly-1.0.jar

## Execution

        analyze (who.cares.the.version - number RC)
        Usage: analyze [options]

          -a <value> | --alpha <value>
                Damping factor of PR & PPR default 0.15
          -i <value> | --iter <value>
                Max iteration number of PR & PPR default 15
          -k <value> | --topk <value>
                TopK elements that will return default 20
          -c <value> | --c <value>
                Parameter for backward PPR, part of decay factor \beta default 50
          --lpaStart <value>
                Min iteration of LPA
          --lpaStep <value>
                Step of LPA iteration
          --lpaEnd <value>
                Max iteration of LPA community detection
          -q <value> | --query <value>
                Query id, usually the starting point of PPR, default is 0l
          -e <value> | --edge <value>
                Path of edge file, should be on hdfs
          -t <value> | --title <value>
                Path of title-id map, should be on local disk
          -o <value> | --output <value>
                Output path, should be a local location
          -n <value> | --ncore <value>
                Number of cores for Spark

## Example

        spark-submit --class edu.nd.dsg.bshi.Main --master yarn-client --num-executors 6 \ 
         --driver-memory 12g --executor-memory 14g --executor-cores 8 analyze-assembly-1.0.jar \
         -e hdfs://dsg1.crc.nd.edu/user/bshi/dblp/citation_edge.txt -o ./text_result.csv --lpaStart 10 --lpaEnd 11