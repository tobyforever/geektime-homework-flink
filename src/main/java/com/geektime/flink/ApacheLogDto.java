package com.geektime.flink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 *    * in: 79.185.184.23 - - 17/05/2015:12:05:31 +0000 GET /reset.css
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLogDto {
    private String ipAddr;
    private Long timestamp;
    private String method;
    private String url;

}
