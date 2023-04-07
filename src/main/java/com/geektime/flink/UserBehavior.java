package com.geektime.flink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior  {
    private Long userId;
    private Long itemId;
    private Long categoryId;
    private String behavior;
    private Long timestamp;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserBehavior that = (UserBehavior) o;
        return Objects.equals(getUserId(), that.getUserId()) && Objects.equals(getItemId(), that.getItemId()) && Objects.equals(getCategoryId(), that.getCategoryId()) && Objects.equals(getBehavior(), that.getBehavior()) && Objects.equals(getTimestamp(), that.getTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUserId(), getItemId(), getCategoryId(), getBehavior(), getTimestamp());
    }
}
