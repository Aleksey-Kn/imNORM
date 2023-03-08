package support.dto;

import io.github.alekseykn.imnorm.annotations.Id;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Dto {
    @Id
    private int id;
}
