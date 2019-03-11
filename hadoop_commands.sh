export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar 

# to get the data
curl -L https://www.dropbox.com/s/4s7do56blmf8cz8/dblp_tsv.tar.gz -o /tmp/dblp_tsv.tar.gz
cd /tmp
tar -xzvf dblp_tsv.tar.gz

# to use hadoop
cd $HADOOP_PREFIX

# to setup the environment
bin/hadoop fs -mkdir -p /user/homework1
bin/hadoop fs -put /tmp/dblp_tsv/papers.tsv /user/homework1/papers.tsv
bin/hadoop fs -put /tmp/dblp_tsv/paperauths.tsv /user/homework1/paperauths.tsv

# first exercise
# to make it simpler to import the code, I put it online on my github and used the curl command
curl -L https://raw.githubusercontent.com/ombelinelage/dmINF583/master/PageCount.java -o PageCount.java

bin/hadoop com.sun.tools.javac.Main PageCount.java
jar cf pagecount.jar PageCount*.class
bin/hadoop jar pagecount.jar PageCount /user/homework1/papers.tsv /user/homework1/page_count_out/

# to get the results
bin/hadoop fs -cat /user/homework1/page_count_out/part-r-00000


# second exercise
curl -L https://raw.githubusercontent.com/ombelinelage/dmINF583/master/Top5.java -o Top5.java

# We can now run it
bin/hadoop com.sun.tools.javac.Main Top5.java
jar cf top5.jar Top5*.class
bin/hadoop jar top5.jar Top5 /user/homework1/paperauths.tsv /user/homework1/author_top5_out/

bin/hadoop fs -cat /user/homework1/author_top5_out/part-r-00000