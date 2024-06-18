package io.github.jbellis;

record RowData(String _id, String url, String title, String text, com.datastax.oss.driver.api.core.data.CqlVector<Float> embedding) {}