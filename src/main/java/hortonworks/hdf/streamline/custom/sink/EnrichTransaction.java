package hortonworks.hdf.streamline.custom.sink;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

import hortonworks.hdf.streamline.events.EnrichedTransaction;
import hortonworks.hdf.streamline.events.IncomingTransaction;
import hortonworks.hdf.streamline.events.Product;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichTransaction implements CustomProcessorRuntime {
	
	protected static final Logger LOG = LoggerFactory.getLogger(EnrichTransaction.class);
	
	private static final String CONFIG_CONNECTION_USER_ID= "connectionUserId";
	private static final String CONFIG_CONNECTION_PASSWORD="connectionPassword";
	private static final String CONFIG_CONNECTION_URL="connectionUrl";
	private static final String CONFIG_TOPIC_NAME = "websocketTopicName";
	
	private Connection conn = null;
	
	private String zkHost = "retaildemo02-306-1-0.field.hortonworks.com"; 
	private String zkPort = "2181";
	private String zkHBasePath ="/hbase-unsecure";
	
	public List<Result> process(StreamlineEvent event) throws ProcessingException {
		List<StreamlineEvent> eventList = new ArrayList<StreamlineEvent>();
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("StreamLine event's field values are: " + event.getAuxiliaryFieldsAndValues());
		}
		 
		JSONObject json = new JSONObject();
        json.putAll(event.getAuxiliaryFieldsAndValues());	
        
        String message = json.toJSONString();
        IncomingTransaction incomingTransaction = null;
        EnrichedTransaction enrichedTransaction = new EnrichedTransaction();
        ObjectMapper mapper = new ObjectMapper();
        System.out.println("******************** Recieved IncomingTransaction... \n value: " + message);
        try {
			incomingTransaction = mapper.readValue(message, IncomingTransaction.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		enrichedTransaction.setAccountNumber(incomingTransaction.getAccountNumber());
		enrichedTransaction.setAccountType(incomingTransaction.getAccountType());
		enrichedTransaction.setAmount(incomingTransaction.getAmount());
		enrichedTransaction.setCurrency(incomingTransaction.getCurrency());
		enrichedTransaction.setIsCardPresent(incomingTransaction.getIsCardPresent());
		enrichedTransaction.setTransactionId(incomingTransaction.getTransactionId());
		enrichedTransaction.setTransactionTimeStamp(incomingTransaction.getTransactionTimeStamp());
		enrichedTransaction.setIpAddress(incomingTransaction.getIpAddress());
		enrichedTransaction.setShipToState(incomingTransaction.getShipToState());
		
		System.out.println("********************** Enriching event: " + incomingTransaction);	    
	    Result result = null;
	    ResultSet resultSet = null;
	    Boolean matchedProduct = false;
	    Boolean matchedLocation = false;
	    try {
	    	Map<String, Object> locValues = new HashMap<String, Object>();
	    	resultSet = conn.createStatement().executeQuery("SELECT * FROM \"Location\" WHERE \"locationId\" = '" + incomingTransaction.getLocationId() + "'");
	    	while (resultSet.next()) {
	    		System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
	    		enrichedTransaction.setLocationId(resultSet.getString("locationId"));
	    		enrichedTransaction.setStreetAddress(resultSet.getString("address"));
	    		enrichedTransaction.setCity(resultSet.getString("city"));
	    		enrichedTransaction.setState(resultSet.getString("state"));
	    		enrichedTransaction.setZipCode(resultSet.getString("zip"));
	    		enrichedTransaction.setLatitude(resultSet.getString("latitude"));
	    		enrichedTransaction.setLongitude(resultSet.getString("longitude"));
	    		enrichedTransaction.setBrand(resultSet.getString("brand"));
	    		
	    		locValues.put("locationId",resultSet.getString("locationId"));
	    		locValues.put("address",resultSet.getString("address"));
	    		locValues.put("city",resultSet.getString("city"));
	    		locValues.put("state",resultSet.getString("state"));
	    		locValues.put("zip",resultSet.getString("zip"));
	    		locValues.put("latitude",resultSet.getString("latitude"));
	    		locValues.put("longitude",resultSet.getString("longitude"));
	    		locValues.put("brand",resultSet.getString("brand"));
	    		matchedLocation = true;
	    	}
	    	
	    	EnrichedTransaction enrichedTransaction2;
	    	Map<String, Object> fieldsAndValues;
	    	
			List<Product> products = new ArrayList<Product>();
			resultSet = conn.createStatement().executeQuery("SELECT * FROM \"Product\" WHERE \"productId\" IN ('" + String.join("','", incomingTransaction.getItems()) + "')");
			while (resultSet.next()) {
				fieldsAndValues = locValues;
				enrichedTransaction2 = enrichedTransaction;
				System.out.println("******************** " + 
    					resultSet.getString("productId") + "," +
    					resultSet.getString("productCategory") + "," +
    					resultSet.getString("productSubCategory") + "," +
    					resultSet.getString("manufacturer") + "," +
    					resultSet.getString("productName") + "," + 
    					resultSet.getDouble("price"));
		    	products.add(new Product(
		    			resultSet.getString("productId"),
		    			resultSet.getString("productCategory"),
		    			resultSet.getString("productSubCategory"),
		    			resultSet.getString("manufacturer"),
		    			resultSet.getString("productName"), 
		    			resultSet.getDouble("price")));
		    	matchedProduct = true;
		    	
		    	enrichedTransaction2.setProductCategory(resultSet.getString("productId"));
		    	enrichedTransaction2.setProductCategory(resultSet.getString("productCategory"));
		    	enrichedTransaction2.setProductSubCategory(resultSet.getString("productSubCategory"));
		    	enrichedTransaction2.setManufacturer(resultSet.getString("manufacturer"));
		    	enrichedTransaction2.setProductName(resultSet.getString("productName"));
		    	enrichedTransaction2.setPrice(resultSet.getDouble("price"));
		    	
		        fieldsAndValues.put("productId", resultSet.getString("productId"));
		        fieldsAndValues.put("productCategory",resultSet.getString("productCategory"));
		        fieldsAndValues.put("peoductSubCategory",resultSet.getString("productSubCategory"));
		        fieldsAndValues.put("manufacturer",resultSet.getString("manufacturer"));
		        fieldsAndValues.put("productName",resultSet.getString("productName"));
		        fieldsAndValues.put("price",resultSet.getDouble("price"));
		    	
		    	
		    	if(matchedLocation && matchedProduct){
					System.out.println("********************** EnrichTransaction execute() emitting Tuple");
				}
				else{
					System.out.println("The transaction refers to a Product and/or Location that are not in the data store.");
					System.out.println("Account: " + incomingTransaction.getAccountNumber());
				}
				
		    	StreamlineEvent enrichedEvent = event.addFieldsAndValues(fieldsAndValues);
				eventList.add(enrichedEvent);
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	    Result resultingEvents = new Result("default", eventList);
	    List<Result> allResults = new ArrayList<Result>();
	    allResults.add(resultingEvents);
	    
        return allResults;
	}

	public void initialize(Map<String, Object> config) {
	
		LOG.info("Initializing Enrichment custom processing");
		//this.user = (String) config.get(CONFIG_CONNECTION_USER_ID);
		//this.password = (String) config.get(CONFIG_CONNECTION_PASSWORD);
		
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection("jdbc:phoenix:"+ zkHost + ":" + zkPort + ":" + zkHBasePath);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		LOG.info("Finished Initializing Enrichment custom processing");
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void validateConfig(Map<String, Object> config)
			throws ConfigException {
		// TODO Auto-generated method stub

	}
}
