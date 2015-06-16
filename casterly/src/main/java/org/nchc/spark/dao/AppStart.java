package org.nchc.spark.dao;

import com.google.gson.annotations.SerializedName;

/**
 * Created by 1403035 on 2015/5/5.
 */
public class AppStart{
    private String Event;
    @SerializedName("App Name") private String App_Name;
    @SerializedName("App ID") private String App_ID;
    private long Timestamp;
    private String User;

    public String getEvent() {
        return Event;
    }

    public String getApp_Name() {
        return App_Name;
    }

    public String getApp_ID() {
        return App_ID;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public String getUser() {
        return User;
    }
}