package core

import (
	"config-center/resource"
	"log"
	"time"
)

type Group struct {
	Name string
	ConfigMap map[string]string
	sessions []*Session
}

type Env struct {
	Name string
	groupMap map[string]*Group
}

func (env *Env) AddGroup(name string, group *Group)  {
	env.groupMap[name] = group
}

func (env *Env) GetGroup(name string) *Group  {
	return env.groupMap[name]
}

func (env *Env) New(name string) Env {
	return Env {
		Name:     name,
		groupMap: make(map[string]*Group),
	}
}

func (group *Group) notify(key string, value string)  {
	for index := range group.sessions {
		go group.sessions[index].receive(key, value)
	}
}

func (env *Env) WriteInDB() {
	db := resource.GetDB(postgres)
	var columnList []string
	var valueList [][]string
	for {
		if len(env.groupMap) != 0 {
			err := resource.UpdateIfConflict(db, configTable, columnList, valueList, "")
			if err != nil {
				log.Println("Error ", err)
			}
		}
		time.Sleep(15 * time.Second)
	}
}
