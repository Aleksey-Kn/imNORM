package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.dto.Dto;
import support.dto.DtoWithGenerateId;

public class FastRepositoryTest extends RepositoryTest{
    @BeforeAll
    static void setRepository() {
        repository = DataStorage.getDataStorage().getStrictlyFastRepositoryForClass(Dto.class);
        withGenerateIdRepository = DataStorage.getDataStorage()
                .getStrictlyFastRepositoryForClass(DtoWithGenerateId.class);
    }
}
