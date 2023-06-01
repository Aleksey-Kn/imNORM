package io.github.alekseykn.imnorm.utils;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import support.dto.Dto;
import support.dto.StringDto;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ClusterFileManipulatorTest {
    private static final File file = new File("test.txt");
    private static Field intId, stringId;

    @BeforeAll
    @SneakyThrows
    static void idInitializer() {
        intId = Dto.class.getDeclaredField("id");
        intId.setAccessible(true);
        stringId = StringDto.class.getDeclaredField("id");
        stringId.setAccessible(true);
    }

    @AfterAll
    static void removeFiles() {
        file.delete();
    }

    @Test
    @SneakyThrows
    void readWithoutCollision() {
        PrintWriter printWriter = new PrintWriter(file);
        printWriter.println("10:{\"id\":10}#");
        printWriter.println("20:{\"id\":20}#");
        printWriter.close();
        Map<Integer, Map<Object, Dto>> expected = new TreeMap<>();
        expected.put(10, Map.of(10, new Dto(10)));
        expected.put(20, Map.of(20, new Dto(20)));

        ClusterFileManipulator<Dto> manipulator = new ClusterFileManipulator<>(Dto.class, intId);

        assertThat(manipulator.read(file.toPath())).containsAllEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void writeWithoutCollision() {
        ClusterFileManipulator<Dto> manipulator = new ClusterFileManipulator<>(Dto.class, intId);
        TreeMap<Integer, Map<Object, Dto>> actual = new TreeMap<>();
        actual.put(25, Map.of(25, new Dto(25)));
        actual.put(-268, Map.of(-268, new Dto(-268)));

        manipulator.write(file, actual);

        assertThat(Files.lines(file.toPath()).reduce("", (result, now) -> result.concat(now).concat("\n")))
                .isEqualTo("""
                        -268:{"id":-268}#
                        25:{"id":25}#
                        """);
    }

    @Test
    @SneakyThrows
    void writeWithCollision() {
        String first = UUID.randomUUID().toString();
        int hashCode = first.hashCode();
        String second = UUID.randomUUID().toString();
        String third = UUID.randomUUID().toString();
        TreeMap<Integer, Map<Object, StringDto>> actual = new TreeMap<>();
        actual.put(hashCode, Map.of(first, new StringDto(first), second, new StringDto(second)));
        actual.put(hashCode + 1, Map.of(third, new StringDto(third)));
        ClusterFileManipulator<StringDto> manipulator = new ClusterFileManipulator<>(StringDto.class, stringId);

        manipulator.write(file, actual);

        String structire = "%d:{\"id\":\"%s\"}#{\"id\":\"%s\"}#\n%d:{\"id\":\"%s\"}#\n";
        assertThat(Files.lines(file.toPath()).reduce("", (result, now) -> result.concat(now).concat("\n")))
                .containsAnyOf(String.format(structire, hashCode, first, second, hashCode + 1, third),
                        String.format(structire, hashCode, second, first, hashCode + 1, third));
    }

    @Test
    @SneakyThrows
    void readWithCollision() {
        PrintWriter printWriter = new PrintWriter(file);
        printWriter.println("10:{\"id\":\"aaa\"}#{\"id\":\"bbb\"}#");
        printWriter.println("20:{\"id\":\"abcd\"}#");
        printWriter.println("892:{\"id\":\"gdfgdfgs\"}#{\"id\":\"abcasertrdfad\"}#{\"id\":\"abcgfagergdfvbsrthd\"}#");
        printWriter.close();
        TreeMap<Integer, Map<Object, StringDto>> expected = new TreeMap<>();
        expected.put(10, Map.of("aaa", new StringDto("aaa"), "bbb", new StringDto("bbb")));
        expected.put(20, Map.of("abcd", new StringDto("abcd")));
        expected.put(892, Map.of("gdfgdfgs", new StringDto("gdfgdfgs"),
                "abcasertrdfad", new StringDto("abcasertrdfad"),
                "abcgfagergdfvbsrthd", new StringDto("abcgfagergdfvbsrthd")));

        ClusterFileManipulator<StringDto> manipulator = new ClusterFileManipulator<>(StringDto.class, stringId);

        assertThat(manipulator.read(file.toPath())).containsAllEntriesOf(expected);
    }
}