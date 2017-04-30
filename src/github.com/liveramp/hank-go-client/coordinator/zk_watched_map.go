package coordinator

type ZkWatchedMap struct {

	internalMap map[string]*int

}

func (p *ZkWatchedMap) put(key string, value *int) {
	//	TODO
}

func (p *ZkWatchedMap) get(key string)(val *int){
	//	TODO do we need to ensureLoaded?
	return p.internalMap[key]
}

//@Override
//public T put(String key, T value) {
//return internalMap.put(key, value);
//}
//
//@Override
//public T get(Object key) {
//ensureLoaded();
//return internalMap.get(key);
//}
