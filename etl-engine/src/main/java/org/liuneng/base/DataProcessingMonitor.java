package org.liuneng.base;

public interface DataProcessingMonitor {

    String getId();

    long getStartTime();

    long getProcessed();

    long getProcessingRate();

    long getInserted();

    long getInsertingRate();

    long getUpdated();

    long getUpdatingRate();

    long getDeleted();

    long getDeletingRate();


}
