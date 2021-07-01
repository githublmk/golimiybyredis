package util

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	gouuid "github.com/satori/go.uuid"
	"strconv"
	"strings"
	"time"
)

//redis key

var RuleType = ruleType{
	QPS:             1,
	AbnormalRatio:   2,
	ContinuousError: 3,
}

type ruleType struct {
	QPS             int //qps
	AbnormalRatio   int //异常比例
	ContinuousError int //连续错误
}

func (r ruleType) isRuleTypeMember(member int) bool {
	switch member {
	case 1, 2, 3:
		return true
	default:
		return false
	}
}

const (
	EXPIRETIME   = "expiretime"   //统计时间
	LIMITTYPE    = "limittype"    //规则方式，2 异常比例、1 QPS、3 单位时间连续异常
	CRITICAL     = "critical"     //临界值
	SLEEPTIME    = "sleeptime"    //恢复时间（窗口期）
	STARTCOUNT   = "startcount"   //达到多少请求开始统计
	RECOVERCOUNT = "recovercount" //半开可以进入的请求数
	HALFTIME     = "halftime"     //半开的窗口时间
	HALFCRITICAL = "halfcritical" //恢复比例

	ISPASS   = "ispass"   //是否达到统计值
	ERRCOUNT = "errcount" //错误数
	COUNT    = "count"    //总数

	HALFERRORCOUNT   = "halferrorcount"   //请求错误的数目
	HALFCORRECTCOUNT = "halfcorrectcount" //请求正确数目
	HALFCOUNT        = "halfcount"        //当前请求数目

)

//服务结构体
type Service struct {
	prefix         string //前缀
	suffix         string //后缀
	halfOpenSetKey string //半开集合key
	redisClient    *redis.ClusterClient
	hashKey        string // 存取资源key
}

//限流处理结构体
type Pass struct {
	service   *Service
	resource  string //资源
	limitType int    //规则类型
	count     int    //可以调用次数
	sid       string //唯一标识

}

//创建限流服务
func NewService(prefix string, suffix string, setKey string, hashKey string, redisClient *redis.ClusterClient) (*Service, error) {
	if redisClient == nil {
		return nil, errors.New("redisClient is nil")
	}
	err := redisClient.Ping().Err()

	if err != nil {
		return nil, err
	}
	s := &Service{
		prefix:         prefix,
		suffix:         suffix,
		halfOpenSetKey: setKey,
		redisClient:    redisClient,
		hashKey:        hashKey,
	}
	return s, nil

}

//注册资源
func (s *Service) AddResource(resource string, expireTime int, limitType int, critical float64, sleepTime int, startCount int, recoverCount int, halfTime int, halfCritical float64) error {
	if !RuleType.isRuleTypeMember(limitType) {
		return errors.New("该规则类型不存在")
	}

	resourceHashKey := s.resourceHashKey(resource)
	info := make(map[string]interface{}, 10)
	info[EXPIRETIME] = expireTime
	info[LIMITTYPE] = limitType
	info[CRITICAL] = critical
	info[SLEEPTIME] = sleepTime
	info[STARTCOUNT] = startCount
	info[RECOVERCOUNT] = recoverCount
	info[HALFTIME] = halfTime
	info[HALFCRITICAL] = halfCritical

	set := s.redisClient.HMSet(resourceHashKey, info)
	result, err := set.Result()
	fmt.Println(result)
	fmt.Println(err)
	return err

}

//是否通过，没有限流视为通过
func (s *Service) Take(resource string) (*Pass, int,string, bool, error) {
	sid := uuid()
	limitType, islimit, err := s.isLimit(resource)
	if err != nil {
		return nil, limitType,sid, false, err
	}

	p := &Pass{
		service:   s,
		resource:  resource,
		limitType: limitType,
		count:     1,
		sid:       sid,
	}
	switch limitType {
	case 1:
		if islimit {
			return nil, limitType,sid, false, nil
		} else {
			return nil, limitType,sid, true, nil
		}
	default:
		if islimit {
			return nil, limitType,sid, false, nil
		} else {
			return p, limitType,sid, true, nil
		}

	}

}

//加锁 放几个请求，其他的按熔断处理
//判断资源是否限流
//判断资源的限流方式
//1.限流 2.异常比例 3 连续错误
func (s *Service) isLimit(resource string) (int, bool, error) {
	var err error
	resourcekey := s.resourceKey(resource)
	resourceHashKey := s.resourceHashKey(resource)
	halfOpenKey := s.halfOpenKey()
	halfRecoverKey := s.resourceHalfRecover(resource)
	info := s.redisClient.HGetAll(resourceHashKey).Val()
	if len(info) != 8 {
		return 0, false, errors.New("获取值缺失")
	}
	resourceInfo := s.redisClient.HGetAll(resourcekey).Val()
	expireTime, err := strconv.Atoi(info[EXPIRETIME])
	halfTime, err := strconv.Atoi(info[HALFTIME])
	limitType, err := strconv.Atoi(info[EXPIRETIME])
	critical, err := strconv.Atoi(info[CRITICAL])
	recoverCount, err := strconv.Atoi(info[RECOVERCOUNT])
	//startCount,err:=strconv.Atoi(info[STARTCOUNT])
	count := 0
	isPass := "false"
	if resourceInfo != nil {
		count, err = strconv.Atoi(resourceInfo[COUNT])
		isPass = resourceInfo[ISPASS]
	}
	if err != nil {
		return 0, false, err
	}
	if limitType == 1 {
		if resourceInfo == nil {
			s.redisClient.HSet(resourcekey, ISPASS, "false")
			s.redisClient.HIncrBy(resourcekey, ERRCOUNT, 0)
			s.redisClient.HIncrBy(resourcekey, COUNT, 1)
			s.redisClient.Expire(resourcekey, time.Duration(expireTime)*time.Second)
			return limitType, true, err
		} else {
			if count+1 > critical {
				return limitType, true, err
			} else {
				s.redisClient.HIncrBy(resourcekey, COUNT, 1)
				return limitType, false, err
			}
		}
	} else {

		if isPass == "true" {
			return limitType, true, err
		}
		isMember := s.redisClient.SIsMember(halfOpenKey, resourcekey).Val()
		recoverInfo := s.redisClient.HGetAll(halfRecoverKey).Val()
		if isMember {
			if recoverInfo == nil {
				if 1 > recoverCount {
					return limitType, true, err
				} else {
					if s.redisClient.Exists(halfRecoverKey).Val() == 1 {
						s.redisClient.HIncrBy(halfRecoverKey, HALFCOUNT, 1)
					} else {
						s.redisClient.HMSet(halfRecoverKey, map[string]interface{}{
							HALFERRORCOUNT:   0,
							HALFCORRECTCOUNT: 0,
							HALFCOUNT:        1,
						})
						s.redisClient.Expire(halfRecoverKey, time.Duration(halfTime)*time.Second)
					}
					return limitType, false, err
				}
			} else {
				halfCount, err := strconv.Atoi(recoverInfo[HALFCOUNT])
				if halfCount+1 > recoverCount {
					return limitType, true, err
				} else {
					s.redisClient.HIncrBy(halfRecoverKey, HALFCOUNT, 1)
					return limitType, false, err
				}
			}

		} else {
			return limitType, false, err
		}

	}

}

//获取资源的redis key
func (s *Service) resourceKey(resource string) string {
	return s.prefix + "_" + resource + "_" + s.suffix
}

//获取保存资源的过期时间、熔断值、熔断方式的rediskey
func (s *Service) resourceHashKey(resource string) string {
	return s.prefix + "_" + s.hashKey + "_" + resource + "_" + s.suffix
}

//获取半开集合在redis中key
func (s *Service) halfOpenKey() string {
	return s.prefix + "_" + s.halfOpenSetKey + "_" + s.suffix
}

//判断是否在半开集合中
func (s *Service) isHalfOpens(resource []string) ([]bool, error) {
	var result []bool
	script := `local rs={}
		 for word in string.gmatch(ARGV[1], "[%w_]+")
			do 
				if redis.call("SISMEMBER",KEYS[1],word)==1
				then
					table.insert(rs,"true") 
				else
					table.insert(rs,"false")
				end
			end
		return rs
`
	eval := s.redisClient.Eval(script, []string{s.halfOpenKey()}, strings.Join(resource, " "))
	res, err := eval.Result()
	if err != nil {
		return result, err
	}
	for _, v := range res.([]interface{}) {
		if v.(string) == "true" {
			result = append(result, true)
		} else if v.(string) == "false" {
			result = append(result, false)
		}
	}
	if len(result) != len(result) {
		return nil, errors.New("返回的数组元素与传入的数组元素个数不一致")
	}
	return result, nil
}

//判断是否在半开集合中
func (s *Service) isHalfOpen(resource string) (bool, error) {
	resourceKey := s.resourceKey(resource)
	halfOpenKey := s.halfOpenKey()

	script := `if redis.call("SISMEMBER",KEYS[1],ARGV[1])==1
				then
					return "true"
				else
					return "false"
				end
`
	eval := s.redisClient.Eval(script, []string{halfOpenKey}, resourceKey)
	res, err := eval.Result()
	if err != nil {
		return false, err
	}

	if res.(string) == "true" {
		return true, nil
	} else if res.(string) == "false" {
		return false, nil
	}
	return false, errors.New("未知错误")
}

//资源探测key
func (s *Service) resourceHalfRecover(resource string) string {
	return s.prefix + "_" + "halfrecover" + "_" + resource + "_" + s.suffix
}

//保存资源信息的hash是否存在
func (s *Service) IsExistsResourceHashKey(resource string) (bool, error) {
	key := s.resourceHashKey(resource)

	result, err := s.redisClient.Exists(key).Result()
	if err != nil {
		return false, err
	}
	if result == 1 {
		return true, err
	} else {
		return false, err
	}

}

//生成uuid
func uuid() string {
	var err error
	s := gouuid.Must(gouuid.NewV4(), err).String()
	return fmt.Sprint(time.Now().Local().UnixNano(), "-", s)
}

func (s *Service) resourceRequestSetKey(resource string) string {
	return s.prefix + "_" + "requestset" + "_" + resource + "_" + s.suffix
}

//对资源进行熔断或恢复处理
func (p *Pass) Pass(isPass bool) error {
	var err error
	if p.count != 1 {
		return errors.New("请勿重复反馈")
	}
	p.count = p.count - 1
	//通过，考虑是否恢复
	resourceKey := p.service.resourceKey(p.resource)
	resourceHashKey := p.service.resourceHashKey(p.resource)
	halfOpenKey := p.service.halfOpenKey()
	resourceHalfRecoverKey := p.service.resourceHalfRecover(p.resource)
	if p.limitType == 1 {
		return nil
	}
	info := p.service.redisClient.HGetAll(resourceHashKey).Val()
	resourceInfo := p.service.redisClient.HGetAll(resourceKey).Val()
	recoverInfo := p.service.redisClient.HGetAll(resourceHalfRecoverKey).Val()
	expireTime, err := strconv.Atoi(info[EXPIRETIME])
	startCount, err := strconv.Atoi(info[STARTCOUNT])
	criticalInt, err := strconv.Atoi(info[CRITICAL])
	sleepTime, err := strconv.Atoi(info[SLEEPTIME])
	CriticalFloat, err := strconv.ParseFloat(info[CRITICAL], 64)
	halfCritical, err := strconv.ParseFloat(info[HALFCRITICAL], 64)
	recoverCountFloat, err := strconv.ParseFloat(info[RECOVERCOUNT], 64)
	count := 0
	if resourceInfo != nil {
		count, err = strconv.Atoi(info[COUNT])

	}

	if err !=nil{
		return err
	}
	if isPass {
		if recoverInfo == nil {
			if resourceInfo == nil {
				p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
					ERRCOUNT: 0,
					COUNT:    1,
					ISPASS:   "false",
				})
				p.service.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
			} else {
				p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
				if p.limitType == 3 {
					p.service.redisClient.HSet(resourceKey, ERRCOUNT, 0)
				}

			}
		} else {
			correctCount, err := strconv.ParseFloat(recoverInfo[HALFCORRECTCOUNT], 64)
			if err !=nil{
				return err
			}
			//halfCount,err := strconv.ParseFloat(recoverInfo[HALFCOUNT],64)
			if (correctCount + 1/recoverCountFloat) >= halfCritical {
				p.service.redisClient.Del(resourceHalfRecoverKey)
				p.service.redisClient.SRem(halfOpenKey, resourceKey)
			} else {
				p.service.redisClient.HIncrBy(resourceHalfRecoverKey, HALFCORRECTCOUNT, 1)
			}
		}

	} else {

		//
		if recoverInfo != nil {
			halfErrorCount, err := strconv.ParseFloat(recoverInfo[HALFERRORCOUNT], 64)
			if err !=nil{
				return err
			}
			if (halfErrorCount + 1/recoverCountFloat) > (1 - halfCritical) {
				p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
					ERRCOUNT: 0,
					COUNT:    0,
					ISPASS:   "true",
				})
				p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
			} else {
				p.service.redisClient.HIncrBy(resourceHalfRecoverKey, HALFERRORCOUNT, 1)
			}

		} else {
			if startCount <= count+1 {
				//-----------------------------------
				if resourceInfo == nil {

					if p.limitType == 2 {
						if 1 > CriticalFloat {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 0,
								COUNT:    0,
								ISPASS:   "true",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 1,
								COUNT:    1,
								ISPASS:   "false",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
						}
					}
					if p.limitType == 2 {
						if 1 > criticalInt {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 0,
								COUNT:    0,
								ISPASS:   "true",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 1,
								COUNT:    1,
								ISPASS:   "false",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
						}
					}

				} else {
					errorCount, err := strconv.ParseFloat(resourceInfo[ERRCOUNT], 64)
					num, err := strconv.ParseFloat(resourceInfo[COUNT], 64)
					if err !=nil{
						return err
					}
					if p.limitType == 2 {
						if (errorCount + 1/num + 1) > CriticalFloat {
							p.service.redisClient.HSet(resourceKey, ISPASS, "true")
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
							p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
						}
					}
					if p.limitType == 3 {
						if (errorCount) > CriticalFloat {
							p.service.redisClient.HSet(resourceKey, ISPASS, "true")
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
							p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
						}
					}

				}
				//	=====================================
			} else {
				if resourceInfo == nil {
					p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
						ERRCOUNT: 1,
						COUNT:    1,
						ISPASS:   "false",
					})
					p.service.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
				} else {
					p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
					p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
				}

			}

		}

	}
	return err
}
func (p *Pass) SID() string {
	return p.sid
}


