package support.dto;

import io.github.alekseykn.imnorm.annotations.Id;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Dto {
    @Id
    private int id;
}
