package io.github.alekseykn.imnorm.utils;

import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;
import io.github.alekseykn.imnorm.exceptions.IllegalFieldNameException;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;

/**
 * Util for execute reflex manipulations with entity fields
 *
 * @author Aleksey-Kn
 */
public class FieldUtil {
    /**
     * Find id field from specified entity
     *
     * @param clas Entity class
     * @return Id field
     * @throws CountIdException Id field not found or more one
     */
    public static Field getIdField(final Class<?> clas) {
        Field[] fields;
        for (Class<?> now = clas; !now.equals(Object.class); now = now.getSuperclass()) {
            fields = Arrays.stream(now.getDeclaredFields())
                    .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                    .toArray(Field[]::new);
            if(fields.length == 1) {
                fields[0].setAccessible(true);
                return fields[0];
            }
            if (fields.length > 1) {
                throw new CountIdException(now);
            }
        }
        throw new CountIdException(clas);
    }

    /**
     * Find field from specified entity on specified field name
     *
     * @param clas Entity type
     * @param fieldName Field name
     * @return Field with specified field name
     * @throws IllegalFieldNameException Field with specified field name not found
     */
    public static Field getFieldFromName(final Class<?> clas, final String fieldName) {
        Field field;
        for (Class<?> now = clas; !now.equals(Object.class); now = now.getSuperclass()) {
            try {
                field = now.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {}
        }
        throw new IllegalFieldNameException(fieldName, clas);
    }

    /**
     * Execute counting entity fields
     *
     * @param clas Entity type
     * @return Fields quantity
     */
    public static int countFields(final Class<?> clas) {
        int result = 0;
        for (Class<?> now = clas; !now.equals(Object.class); now = now.getSuperclass()) {
            result += now.getDeclaredFields().length;
        }
        return result;
    }
}
