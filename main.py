from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any


@dataclass
class AnimalData:
    id: str
    name: str
    friends: List[str]
    born_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)