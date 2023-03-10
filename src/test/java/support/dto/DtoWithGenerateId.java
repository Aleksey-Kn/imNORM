package support.dto;

import io.github.alekseykn.imnorm.annotations.Id;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class DtoWithGenerateId {
    @Id(autoGenerate = true)
    private int id;

    private final int number;

    public DtoWithGenerateId(final int n) {
        number = n;
    }
}
