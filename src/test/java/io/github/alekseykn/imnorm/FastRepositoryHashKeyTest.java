package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.dto.StringDto;

public class FastRepositoryHashKeyTest extends HashKeyTest{
    @BeforeAll
    static void init() {
        repository = DataStorage.getDataStorage().getStrictlyFastRepositoryForClass(StringDto.class);
    }
}
