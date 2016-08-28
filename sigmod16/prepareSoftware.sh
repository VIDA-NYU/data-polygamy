cd ../data-polygamy/
./install-repositories
mvn clean package
cp target/data-polygamy-0.1-jar-with-dependencies.jar ../sigmod16/data-polygamy.jar
