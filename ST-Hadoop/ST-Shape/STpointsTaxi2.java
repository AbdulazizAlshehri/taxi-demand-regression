package edu.umn.cs.sthadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A data type used to index tweets for Taghreed project.
 *
 * @author Louai Alarabi
 */
public class STpointsTaxi2 extends STPoint {

    /* VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RateCodeID,store_and_fwd_flag
    ,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount */
    private static final Log LOG = LogFactory.getLog(STpointsTaxi2.class);
    // taxi vars
    public int vendorID;
    public int passengerCount;
    public int rateCodeID;
    public String storeAndFwdFlag;
    public int paymentType;
    public float fareAmount;
    public float extra;
    public float mtaTax;
    public float tipAmount;
    public float tollsAmount;
    public float improvementSurcharge;

    public String pickupDateTime; // ST Time
    public String dropoffDateTime;
    public float tripDistance;
    public float pickupLongitude; // ST Longitude
    public float pickupLatitude; // ST Latitude
    public float dropoffLongitude;
    public float dropoffLatitude;
    public float totalAmount;

    public STpointsTaxi2() {
        // TODO Auto-generated constructor stub
    }

    public STpointsTaxi2(String text) throws ParseException {

        String[] list = text.toString().split(",");
        vendorID = Integer.parseInt(list[0]);
        dropoffDateTime = list[2];
        passengerCount = Integer.parseInt(list[3]);
        tripDistance = Float.parseFloat(list[4]);
        rateCodeID = Integer.parseInt(list[7]);
        storeAndFwdFlag = list[8];
        dropoffLongitude = Float.parseFloat(list[9]);
        dropoffLatitude = Float.parseFloat(list[10]);
        paymentType = Integer.parseInt(list[11]);
        fareAmount = Float.parseFloat(list[12]);
        extra = Float.parseFloat(list[13]);
        mtaTax = Float.parseFloat(list[14]);
        tipAmount = Float.parseFloat(list[15]);
        tollsAmount = Float.parseFloat(list[16]);
        improvementSurcharge = Float.parseFloat(list[17]);
        totalAmount = Float.parseFloat(list[18]);
        super.fromText(new Text(list[1] + "," + list[6] + "," + list[5]));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(vendorID);
        out.writeUTF(dropoffDateTime);
        out.writeInt(passengerCount);
        out.writeFloat(tripDistance);
        out.writeInt(rateCodeID);
        out.writeUTF(storeAndFwdFlag);
        out.writeFloat(dropoffLongitude);
        out.writeFloat(dropoffLatitude);
        out.writeFloat(fareAmount);
        out.writeInt(paymentType);
        out.writeFloat(extra);
        out.writeFloat(mtaTax);
        out.writeFloat(tipAmount);
        out.writeFloat(tollsAmount);
        out.writeFloat(improvementSurcharge);
        out.writeFloat(totalAmount);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vendorID = in.readInt();
        dropoffDateTime = in.readUTF();
        passengerCount = in.readInt();
        tripDistance = in.readFloat();
        rateCodeID = in.readInt();
        storeAndFwdFlag = in.readUTF();
        dropoffLongitude = in.readFloat();
        dropoffLatitude = in.readFloat();
        fareAmount = in.readFloat();
        paymentType = in.readInt();
        extra = in.readFloat();
        mtaTax = in.readFloat();
        tipAmount = in.readFloat();
        tollsAmount = in.readFloat();
        improvementSurcharge = in.readFloat();
        totalAmount = in.readFloat();
        super.readFields(in);

    }

    @Override
    public Text toText(Text text) {
        byte[] separator = new String(",").getBytes();
        TextSerializerHelper.serializeInt(vendorID, text, ',');
        text.append(dropoffDateTime.getBytes(), 0, dropoffDateTime.getBytes().length);
        text.append(separator, 0, separator.length);
        TextSerializerHelper.serializeInt(passengerCount, text, ',');
        TextSerializerHelper.serializeDouble(tripDistance, text, ',');
        TextSerializerHelper.serializeInt(rateCodeID, text, ',');
        text.append(dropoffDateTime.getBytes(), 0, storeAndFwdFlag.getBytes().length);
        text.append(separator, 0, separator.length);
        TextSerializerHelper.serializeDouble(dropoffLongitude, text, ',');
        TextSerializerHelper.serializeDouble(dropoffLatitude, text, ',');
        TextSerializerHelper.serializeDouble(fareAmount, text, ',');
        TextSerializerHelper.serializeInt(paymentType, text, ',');
        TextSerializerHelper.serializeDouble(extra, text, ',');
        TextSerializerHelper.serializeDouble(mtaTax, text, ',');
        TextSerializerHelper.serializeDouble(tipAmount, text, ',');
        TextSerializerHelper.serializeDouble(tollsAmount, text, ',');
        TextSerializerHelper.serializeDouble(improvementSurcharge, text, ',');
        TextSerializerHelper.serializeDouble(totalAmount, text, ',');
        super.toText(text);
        return text;
    }

    @Override
    public void fromText(Text text) {
        String[] list = text.toString().split(",");
        vendorID = Integer.parseInt(list[0]);
        dropoffDateTime = list[2];
        passengerCount = Integer.parseInt(list[3]);
        tripDistance = Float.parseFloat(list[4]);
        rateCodeID = Integer.parseInt(list[7]);
        storeAndFwdFlag = list[8];
        dropoffLongitude = Float.parseFloat(list[9]);
        dropoffLatitude = Float.parseFloat(list[10]);
        paymentType = Integer.parseInt(list[11]);
        fareAmount = Float.parseFloat(list[12]);
        extra = Float.parseFloat(list[13]);
        mtaTax = Float.parseFloat(list[14]);
        tipAmount = Float.parseFloat(list[15]);
        tollsAmount = Float.parseFloat(list[16]);
        improvementSurcharge = Float.parseFloat(list[17]);
        totalAmount = Float.parseFloat(list[18]);
        super.fromText(new Text(list[1] + "," + list[6] + "," + list[5]));
    }

    @Override
    public STpointsTaxi2 clone() {
        STpointsTaxi2 c = new STpointsTaxi2();
        c.dropoffDateTime = this.dropoffDateTime;
        c.tripDistance = this.tripDistance;
        c.dropoffLongitude = this.dropoffLongitude;
        c.dropoffLatitude = this.dropoffLatitude;
        c.totalAmount = this.totalAmount;
        c.vendorID = this.vendorID;
        c.passengerCount = this.passengerCount;
        c.rateCodeID = this.rateCodeID;
        c.storeAndFwdFlag = this.storeAndFwdFlag;
        c.paymentType = this.paymentType;
        c.fareAmount = this.fareAmount;
        c.extra = this.extra;
        c.mtaTax = this.mtaTax;
        c.tipAmount = this.tipAmount;
        c.tollsAmount = this.tollsAmount;
        c.improvementSurcharge = this.improvementSurcharge;
        super.set(x, y, time);
        return c;
    }

    public static void main(String[] args) {
        String temp = "2,2015-01-15 19:05:39,2015-01-15 19:23:42,1,1.59,-73.993896484375,40.7501106262207,1,N"
                + ",-73.97478485107422,40.75061798095703,1,12.0,1.0,0.5,3.25,0.0,0.3,17.05";

        STpointsTaxi2 point = new STpointsTaxi2();
        point.fromText(new Text(temp));
        STPoint point3d = (STPoint) point;
        System.out.println(point.time);
        System.out.println(point3d.time);

        // Test casting from 3D to 2D shape.
        Point point2D = (Point) point;
        Text txt = new Text();
        point.toText(txt);
        System.out.println("Point : " + txt.toString());
        System.out.println("Point3D : " + point3d.toString());
        System.out.println("Point2D : " + point2D.toString());

    }

}
