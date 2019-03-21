package org.kududb.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.List;
//import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.LocatedTablet;

public class KuduTableSplit extends FileSplit implements InputSplit {

	private static final Log LOG = LogFactory.getLog(KuduTableSplit.class);

	private byte[] scanTokenSerialized;
	private String[] locations;
	private String columns;

	/**
	 * For Writable
	 */
	public KuduTableSplit() {
		super(null, 0, 0, (String[]) null);
	}

	public KuduTableSplit(KuduScanToken scanToken, Path dummyPath, String columns) throws IOException {
		super(dummyPath, 0, 0, (String[]) null);
		LOG.warn("split fs path: " + super.toString());
		scanTokenSerialized = scanToken.serialize();
		// mark jdk 1.8
//		tableSplit.locations = scanToken.getTablet().getReplicas().stream().map(replica -> replica.getRpcHost())
//				.collect(Collectors.toList()).toArray(new String[0]);
		List<LocatedTablet.Replica> replicas = scanToken.getTablet().getReplicas();
		String[] hosts = new String[replicas.size()];
		for (int i = 0; i < replicas.size(); i++) {
			hosts[i] = replicas.get(i).getRpcHost();
		}
		locations = hosts;
		this.columns = columns;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		LOG.warn("split write fs path: " + super.toString());
		out.writeUTF(this.columns);
		// Write scanTokenSerialized
		out.writeInt(this.scanTokenSerialized.length);
		out.write(this.scanTokenSerialized);
		// Write locations
		out.writeInt(this.locations.length);
		for (String loc : this.locations)
			out.writeUTF(loc);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		LOG.warn("split readFields fs path: " + super.toString());
		this.columns = in.readUTF();
		// read scanTokenSerialized
		int byteArrayLength = in.readInt();
		this.scanTokenSerialized = new byte[byteArrayLength];
		in.readFully(scanTokenSerialized);
		// read locations
		int locationsLength = in.readInt();
		this.locations = new String[locationsLength];
		for (int i = 0; i < locationsLength; ++i)
			this.locations[i] = in.readUTF();
	}

	@Override
	public long getLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		return this.locations.clone();
	}

	byte[] getScanTokenSerialized() {
		return this.scanTokenSerialized.clone();
	}

	String getColumns() {
		return columns;
	}
}
