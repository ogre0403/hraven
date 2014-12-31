package org.nchc.history;

/**
 * Created by 1403035 on 2014/12/31.
 */
public class RunningStatus {
    int mapProgress = 0;
    int  reduceProgress = 0;
    long startTime = 0L;
    long elapsedTime = 0L;
    long ETA = 0L;

    public RunningStatus(int m_progress, int r_progress, long startTime, long elapsedTime, long ETA){
        this.mapProgress = m_progress;
        this.reduceProgress = r_progress;
        this.startTime = startTime;
        this.elapsedTime = elapsedTime;
        this.ETA = ETA;
    }

    @Override
    public String toString() {
        return "RunningStatus{" +
                "mapProgress=" + mapProgress +
                ", reduceProgress=" + reduceProgress +
                ", startTime=" + startTime +
                ", elapsedTime=" + elapsedTime +
                ", ETA=" + ETA +
                '}';
    }

    public int getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(int mapProgress) {
        this.mapProgress = mapProgress;
    }

    public int getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(int reduceProgress) {
        this.reduceProgress = reduceProgress;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public long getETA() {
        return ETA;
    }

    public void setETA(long ETA) {
        this.ETA = ETA;
    }


}
