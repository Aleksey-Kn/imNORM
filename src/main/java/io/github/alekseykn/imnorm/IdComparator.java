package io.github.alekseykn.imnorm;

import lombok.RequiredArgsConstructor;

import java.lang.reflect.Field;
import java.util.Comparator;

@RequiredArgsConstructor
public class IdComparator implements Comparator<Object> {
    private final Field recordId;
    
    @Override
    public int compare(Object first, Object second) {
        try {
            Object firstKey = recordId.get(first);
            Object secondKey = recordId.get(second);
            if (firstKey instanceof Comparable) {
                return ((Comparable) firstKey).compareTo(secondKey);
            } else {
                return String.valueOf(firstKey).compareTo(String.valueOf(secondKey));
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
