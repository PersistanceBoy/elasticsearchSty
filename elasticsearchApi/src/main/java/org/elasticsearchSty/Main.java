package org.elasticsearchSty;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @Author: liu
 * @Description: ${description}
 * @Date: ${DATE} ${TIME}
 */
public class Main {

    final static String companyIp = "192.168.10.142";
    final static String homeIp = "192.168.31.187";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(companyIp, 9200, "http"))
        );
        //creatIndex(client);
        //getIndex(client);

        //creDoc(client);
        //updateDoc(client);
        //getDoc(client);
        batchInsert(client);
        client.close();
    }

    public static void creatIndex(RestHighLevelClient client) {
        try {
            // 创建索引 - 请求对象
            CreateIndexRequest request = new CreateIndexRequest("user");
            // 发送请求，获取响应
            CreateIndexResponse response = client.indices().create(request,
                    RequestOptions.DEFAULT);
            boolean acknowledged = response.isAcknowledged();
            System.out.println(acknowledged);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getIndex(RestHighLevelClient client) {
        try {
            // 查询索引 - 请求对象
            GetIndexRequest request = new GetIndexRequest("user");

// 发送请求，获取响应
            GetIndexResponse response = client.indices().get(request,
                    RequestOptions.DEFAULT);
            System.out.println("aliases:" + response.getAliases());
            System.out.println("mappings:" + response.getMappings());
            System.out.println("settings:" + response.getSettings());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void delIndex(RestHighLevelClient client) {
        try {
            // 删除索引 - 请求对象
            DeleteIndexRequest request = new DeleteIndexRequest("user");
// 发送请求，获取响应
            AcknowledgedResponse response = client.indices().delete(request,
                    RequestOptions.DEFAULT);
// 操作结果
            System.out.println("操作结果 ： " + response.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建数据，添加到文档中
     *
     * @param client
     */
    public static void creDoc(RestHighLevelClient client) {
        try {
            IndexRequest request = new IndexRequest();
            request.index("user").id("1001");
            User user = new User();
            user.setName("zhangsan");
            user.setAge(30);
            user.setSex("男");
            ObjectMapper objectMapper = new ObjectMapper();
            String productJson = objectMapper.writeValueAsString(user);
// 添加文档数据，数据格式为 JSON 格式
            request.source(productJson, XContentType.JSON);
// 客户端发送请求，获取响应对象
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
////3.打印结果信息
            System.out.println("_index:" + response.getIndex());
            System.out.println("_id:" + response.getId());
            System.out.println("_result:" + response.getResult());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取文档中
     *
     * @param client
     */
    public static void getDoc(RestHighLevelClient client) {
        try {
            //1.创建请求对象
            GetRequest request = new GetRequest().index("user").id("1001");
//2.客户端发送请求，获取响应对象
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
////3.打印结果信息
            System.out.println("_index:" + response.getIndex());
            System.out.println("_type:" + response.getType());
            System.out.println("_id:" + response.getId());
            System.out.println("source:" + response.getSourceAsString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改数据，添加到文档中
     *
     * @param client
     */
    public static void updateDoc(RestHighLevelClient client) {
        try {
            // 修改文档 - 请求对象
            UpdateRequest request = new UpdateRequest();
// 配置修改参数
            request.index("user").id("1001");
// 设置请求体，对数据进行修改
            request.doc(XContentType.JSON, "sex", "女");
// 客户端发送请求，获取响应对象
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            System.out.println("_index:" + response.getIndex());
            System.out.println("_id:" + response.getId());
            System.out.println("_result:" + response.getResult());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量新增
     *
     * @param client
     */
    public static void batchInsert(RestHighLevelClient client) {
        try {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name",
                    "zhangsan"));
            request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name",
                    "lisi"));
            request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name",
                    "wangwu"));

            IndexRequest requestindex = new IndexRequest();
            requestindex.index("user").id("1004");
            User user = new User();
            user.setName("张四");
            user.setAge(39);
            user.setSex("男");
            ObjectMapper objectMapper = new ObjectMapper();
            String productJson = objectMapper.writeValueAsString(user);
            // 添加文档数据，数据格式为 JSON 格式
            requestindex.source(productJson, XContentType.JSON);
            request.add(requestindex);


//客户端发送请求，获取响应对象
            BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
//打印结果信息
            System.out.println("took:" + responses.getTook());
            System.out.println("items:" + responses.getItems());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}