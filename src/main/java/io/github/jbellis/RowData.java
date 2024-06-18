package io.github.jbellis;

import java.util.List;

record RowData(String _id, String url, String title, String text, List<Float> embedding) {}