package RecommendationSystem


import PredictionCollection.Prediction
import com.google.maps.model.LatLng
import com.google.maps.{GeoApiContext, GeocodingApi}
import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.Imports._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by avalj on 12/08/16.
  */

case class Location(latitude: Double, longtitude: Double)

case class Station(id: String, coordinates: (Double, Double), elevation: String)

case class Temperature(id: String, GeoLocation: String, category: String, value: Double)

case class Pressure(id: String, GeoLocation: String, time: String, value: Double)

case class Weather(Place: String, Temp_max: Double, Temp_min: Double, weather_type: String)

object WeatherCollection {
  val api_key = "AIzaSyB-PHZw-tedW3wepxkKVEmcjDVZSApQOjI"

  def getGeoCoordinate(placeOfInterest: String): Location = {
    //val api_key = "AIzaSyBRi4V1Zppq2jI220OBHY0mELI7yQhlzHo"
    val context = new GeoApiContext().setApiKey(api_key)
    val results = GeocodingApi.geocode(context, placeOfInterest).await()
    val latNlong = results(0).geometry.location
    Location(latNlong.lat, latNlong.lng)
  }

  def getGeoLocation(loc: Location): String = {
    //val api_key = "AIzaSyBRi4V1Zppq2jI220OBHY0mELI7yQhlzHo"
    val context = new GeoApiContext().setApiKey(api_key)
    val latNlong = new LatLng(loc.latitude, loc.longtitude)
    val results = GeocodingApi.reverseGeocode(context, latNlong).await()
    val geoLocation = results(0).addressComponents(2).longName
    geoLocation
  }

  def pressureClass(bar: Double): String = {
    bar match {
      case 0 => "Storm"
      case 1 => "Rain"
      case 2 => "Cloudy"
      case 3 => "Fair"
      case 4 => "Dry"
    }
  }

  def weather(inputPlace: String, radius: Double, nextDate: DateTime): Array[Weather] = {
    val placeOfInterest = inputPlace
    val LocObj = getGeoCoordinate(placeOfInterest)
    val stations_ghcn = getNearestStations(LocObj, radius, "stations_ghcn")
    val stations_ghcn_ids = stations_ghcn.map(s => s.id)
    //ids nearest to place of interest
    val station_isd = getNearestStations(LocObj, radius, "stations_isd")
    val station_isd_ids = station_isd.map(s => s.id) //ids nearest to place of interest

    // Start of application
    //val sc = new SparkContext("local[2]", "project")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Learning and Prediction System").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dtf = DateTimeFormat.forPattern("yyyyMMdd")
    val date_to_predict1 = dtf.print(nextDate)
    val date_to_predict2 = dtf.print(nextDate)

    Prediction.Temperature(stations_ghcn_ids, sc, date_to_predict1)
    Prediction.Pressure(station_isd_ids, sc, date_to_predict2)
    val tmax_all = getTmaxPrediction(stations_ghcn)
    val tmin_all = getTminPrediction(stations_ghcn)
    val bar_all = getPressure(station_isd)

    val temperature_data = new ArrayBuffer[Weather]()
    val weather_data = new ArrayBuffer[Weather]()
    val places1 = new mutable.HashSet[String]()
    val places2 = new mutable.HashSet[String]()
    //val weather_data_unknown = new mutable.HashSet[Weather]()
    tmax_all.foreach { tmax =>
      tmin_all.foreach { tmin =>
        if (tmax.GeoLocation.equals(tmin.GeoLocation))
          if (!places1.contains(tmax.GeoLocation)) {
            places1 += tmax.GeoLocation
            temperature_data += Weather(tmin.GeoLocation, tmax.value, tmin.value, "None")
          }
      }
    }

    temperature_data.foreach { weather =>
      bar_all.foreach { bar =>
        if (weather.Place.equals(bar.GeoLocation)) {
          if (!places2.contains(bar.GeoLocation)) {
            places2 += bar.GeoLocation
            weather_data += Weather(weather.Place, weather.Temp_max, weather.Temp_min, pressureClass(bar.value))
          }
        }
      }
    }
    weather_data.toArray
  }

  def getTmaxPrediction(stations: Array[Station]): Array[Temperature] = {

    val tmax = Source.fromFile("./Prediction/TMAX.txt").getLines()
    var tmaxArray = new ArrayBuffer[Temperature]()
    tmax.foreach { line =>
      val parts = line.split(",")
      val stat = stations.map(x => (x.id, x.coordinates)).filter(x => x._1.contains(parts(0)))
      val coord = stat.map(c => c._2)
      val LocObj = Location(coord(0)._1, coord(0)._2)
      tmaxArray += Temperature(parts(0), getGeoLocation(LocObj), "tmax", parts(2).toDouble)
    }
    tmaxArray.toArray
  }

  def getTminPrediction(stations: Array[Station]): Array[Temperature] = {
    val tmin = Source.fromFile("./Prediction/TMIN.txt").getLines()
    var tminArray = new ArrayBuffer[Temperature]()
    tmin.foreach { line =>
      val parts = line.split(",")
      val stat = stations.map(x => (x.id, x.coordinates)).filter(x => x._1.contains(parts(0)))
      val coord = stat.map(c => c._2)
      val LocObj = Location(coord(0)._1, coord(0)._2)
      tminArray += Temperature(parts(0), getGeoLocation(LocObj), "tmin", parts(2).toDouble)
    }
    tminArray.toArray
  }

  def getPressure(stations_isd: Array[Station]): Array[Pressure] = {
    val bar = Source.fromFile("./Prediction/pressure.txt").getLines()
    var barArray = new ArrayBuffer[Pressure]()
    bar.foreach { line =>
      val parts = line.split(",")
      val stat = stations_isd.map(x => (x.id, x.coordinates)).filter(x => x._1.contains(parts(0)))
      val coord = stat.map(c => c._2)
      val LocObj = Location(coord(0)._1, coord(0)._2)
      barArray += Pressure(parts(0), getGeoLocation(LocObj), parts(1).substring(8, 12), parts(2).toDouble)
    }
    barArray.toArray
  }


  def getNearestStations(location: Location, radius: Double, CollectionName: String): Array[Station] = {

    //Connect to db and get ghcn data
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("bigdataproject")
    val stations_collection = db(CollectionName)

    //Location coordinates
    val lat = location.latitude
    val long = location.longtitude

    //Query with the range of the input radius(in geographical degrees)
    val points = "coordinates" $near(lat, long) $maxDistance radius
    val result = stations_collection.find(points).toArray
    result.map { x => fromBson(x) }.filter(x => x.elevation.toDouble < 600.0)

  }

  def fromBson(obj: DBObject): Station = {
    Station(
      obj.as[String]("station_id"),
      readCoordinates(obj.as[DBObject]("coordinates").toString),
      obj.as[String]("elevation")
    )
  }

  /** Convert coordinates from Bson database output. */
  def readCoordinates(ob: String): (Double, Double) = {
    val latitude = ob.split(",")(0).drop(1).toDouble
    val longtitude = ob.split(",")(1).dropRight(1).toDouble
    (latitude, longtitude)
  }

}
