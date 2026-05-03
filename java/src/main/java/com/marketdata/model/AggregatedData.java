package com.marketdata.model;

public class AggregatedData {
    private String symbol;
    private double lastPrice;
    private double openPrice;
    private double highPrice;
    private double lowPrice;
    private long totalVolume;
    private long updateTime;

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public double getLastPrice() { return lastPrice; }
    public void setLastPrice(double lastPrice) { this.lastPrice = lastPrice; }
    public double getOpenPrice() { return openPrice; }
    public void setOpenPrice(double openPrice) { this.openPrice = openPrice; }
    public double getHighPrice() { return highPrice; }
    public void setHighPrice(double highPrice) { this.highPrice = highPrice; }
    public double getLowPrice() { return lowPrice; }
    public void setLowPrice(double lowPrice) { this.lowPrice = lowPrice; }
    public long getTotalVolume() { return totalVolume; }
    public void setTotalVolume(long totalVolume) { this.totalVolume = totalVolume; }
    public long getUpdateTime() { return updateTime; }
    public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
}
