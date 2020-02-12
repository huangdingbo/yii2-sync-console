<?php


namespace dsj\sync\console\controllers;


use dsj\adminuser\models\Adminuser;
use dsj\components\controllers\BgController;
use dsj\sync\web\models\TSyncDb;
use Yii;
use yii\base\Exception;

class SyncDbCheckController extends BgController
{
    public function actionIndex(){
        return function (){
            $list = TSyncDb::find()->all();

            $userList = Adminuser::find()->all();

            /**
             * @var  $obj TSyncDb
             */
            foreach ($list as $obj){
                try{
                    $db = TSyncDb::getActiveDbById($obj->id);
                    $db->close();
                    $obj->failed_num = 0;
                    $obj->save();
                }catch (Exception $e){
                    $obj->failed_num += 1;
                    if ($obj->failed_num > 3){
                        $messages = [];
                        /**
                         * @var  $user Adminuser
                         */
                        foreach ($userList as $user){
                            $messages[] = Yii::$app->mailer->compose()
                                ->setTo($user->email)
                                ->setSubject('数据库连接检测')
                                ->setTextBody("$obj->name:连接检测失败次数超过3次。错误信息为:{$e->getMessage()}");
                        }
                        Yii::$app->mailer->sendMultiple($messages);
                    }
                    $obj->save();
                }
            }
        };
    }
}