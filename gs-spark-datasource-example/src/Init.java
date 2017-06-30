import java.util.Date;
import java.util.Properties;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.Collection;

public class Init {

	static class Point {
		@RowKey Date timestamp;
		long deviceId;
	}

	static class Device {
		@RowKey long id;
		long userId;
		long year;
	}
	
	public static void main(String[] args) throws GSException {

		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", "admin");
		props.setProperty("password", "admin");
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		//---------------------------------------------
		
		TimeSeries<Point> ts = store.putTimeSeries("point01", Point.class);

		Point point = new Point();
		
		point.timestamp = TimestampUtils.current();
		point.deviceId = 144012;
		ts.put(point);
		
		point.timestamp = TimestampUtils.add(point.timestamp, 1, TimeUnit.SECOND);
		point.deviceId = 24485;
		ts.put(point);
		
		point.timestamp = TimestampUtils.add(point.timestamp, 1, TimeUnit.SECOND);
		point.deviceId = 30292;
		ts.put(point);
		
		point.timestamp = TimestampUtils.add(point.timestamp, 1, TimeUnit.SECOND);
		point.deviceId = 24485;
		ts.put(point);
		
		//---------------------------------------------
		
		Collection<Long, Device> col = store.putCollection("devices", Device.class);

		Device device = new Device();

		device.id = 30292;
		device.userId = 47;
		device.year = 2015;
		col.put(device);
		
		device.id = 107624;
		device.userId = 86;
		device.year = 2016;
		col.put(device);
		
		device.id = 121150;
		device.userId = 47;
		device.year = 2017;
		col.put(device);
		
		device.id = 22975;
		device.userId = 86;
		device.year = 2014;
		col.put(device);
		
		device.id =  144012;
		device.userId = 122;
		device.year = 2017;
		col.put(device);
		
		device.id =  90714;
		device.userId = 77;
		device.year = 2016;
		col.put(device);
		
		device.id =  24485;
		device.userId = 1;
		device.year = 2014;
		col.put(device);

		store.close();
	}

}
