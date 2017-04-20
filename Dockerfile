FROM hazelcast/hazelcast:3.8.1

ENV CLASSPATH $HZ_HOME/lib

ADD src/test/resources/hazelcast.xml $HZ_HOME
ADD src/test/resources/server.sh $HZ_HOME
ADD target/lib $HZ_HOME/lib
ADD target/hazelcast-discovery-ecs-0.1-SNAPSHOT.jar $HZ_HOME/lib/hazelcast-discovery-ecs-0.1-SNAPSHOT.jar

CMD ./server.sh