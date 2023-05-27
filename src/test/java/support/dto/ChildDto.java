package support.dto;

import lombok.Getter;

@Getter
public class ChildDto extends Dto {
    private short odd;

    public ChildDto(int id) {
        super(id);
    }
}
