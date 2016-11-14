package mongodb;

import com.flwrobot.jms.entity.LogApiEntity;
import com.flwrobot.jms.entity.LogApiEntityImpl;
import com.google.gson.Gson;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Copyright (c) 2016 www.yongzhian.cn. All Rights Reserved.
 */
public class MongodbTest {
    private static Logger logger = Logger.getLogger(MongodbTest.class);
    //jms实体存入mongodb
    @Test
    public void jms2MongodbByMongoClientTest() throws Exception {
        LogApiEntityImpl generalLogApiEntity = new LogApiEntityImpl();
        generalLogApiEntity.setInvokeTime(new Date());

        LogApiEntity logApiEntity = new LogApiEntity();
        logApiEntity.setInvokeTime(new Timestamp(System.currentTimeMillis()));

        MongoClient mongoClient = new MongoClient("192.168.1.243",27017);
        MongoDatabase db = mongoClient.getDatabase("dosee_local");

        //默认无直接将一般对象存入mongodb 需通过json桥梁转换
        String json = serialize(logApiEntity); //通过codehua json转换
        Document doc =  Document.parse(json);
        logger.info("json : " + json);

        MongoCollection<Document> dbCollection =  db.getCollection("myEntity");
        dbCollection.insertOne(doc);

        //转换成json字符串，再转换成DBObject对象
        Gson gson=new Gson(); //通过google的Gson进行json转换
        logApiEntity.setInvokedSN("575");
        Document doc1 =  Document.parse(gson.toJson(logApiEntity));
        dbCollection.insertOne(new Document("docd",logApiEntity));

    }

    @Test
    public void jms2MongodbByMongoTest() throws Exception {
        Mongo mongo = new MongoClient("192.168.1.243",27017); //老版本
        DB db = mongo.getDB("dosee_local");
        DBCollection collection =  db.getCollection("myEntity");
//        collection.save()

    }

    @Test
    public void jsm2MongodbBySpring() throws Exception {
        Mongo mongo = new MongoClient("192.168.1.243",27017);
        MongoTemplate mongoTemplate = new MongoTemplate(mongo,"dosee_local");
//插入GeneralLogApiEntity数据
//        GeneralLogApiEntity generalLogApiEntity = new GeneralLogApiEntity();
//        generalLogApiEntity.setInvokeTime(new Date());
//        generalLogApiEntity.setInvokedSN("sprig");
//        mongoTemplate.insert(generalLogApiEntity,"myEntity");

        //取出GeneralLogApiEntity
        LogApiEntityImpl generalLogApiEntity1 =  mongoTemplate.findById("57fc5a05b163777f08a468ca",LogApiEntityImpl.class,"myEntity");
        logger.info(generalLogApiEntity1);

        //取出前期数据LogApiEntity
        LogApiEntityImpl generalLogApiEntity2 =  mongoTemplate.findById("57fb5999318ce10b347fced0",LogApiEntityImpl.class,"http_api_log");
        logger.info(generalLogApiEntity2);
    }

    public static String serialize(Object object) {
        ObjectMapper objectMapper = new ObjectMapper();
        if(null == object) {
            return null;
        }

        if (object instanceof String) {
            return (String) object;
        }

        Writer write = new StringWriter();

        try {
            objectMapper.writeValue(write, object);
        } catch (JsonGenerationException e) {
            logger.error(
                    "JsonGenerationException when serialize object to json", e);
        } catch (JsonMappingException e) {
            logger.error("JsonMappingException when serialize object to json",
                    e);
        } catch (IOException e) {
            logger.error("IOException when serialize object to json", e);
        }
        return write.toString();
    }

}
