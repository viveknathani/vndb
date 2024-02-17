package engine

type RetainKey []byte
type RetainValue interface {
	bool | int64 | float64 | string
}

type Engine[T RetainValue] interface {
	Get() (T, error)
	Set(RetainKey, T) (bool, error)
	Keys() ([]RetainKey, error)
}
