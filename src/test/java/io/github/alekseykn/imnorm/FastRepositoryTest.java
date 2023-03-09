package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.dto.Dto;

public class FastRepositoryTest extends RepositoryTest{
    @BeforeAll
    static void setRepository() {
        repository = DataStorage.getDataStorage().getPreferablyFastRepositoryForClass(Dto.class);
    }
}
