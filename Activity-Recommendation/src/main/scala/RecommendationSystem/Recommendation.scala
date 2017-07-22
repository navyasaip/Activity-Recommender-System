package RecommendationSystem

import scala.collection.mutable.ArrayBuffer

/**
  * Created by avalj on 12/09/16.
  */

case class RecommededPlaces(places: Array[InterestPoint_reformed], weather_data: Weather)

object Recommendation {

  def Recommend(places: Array[InterestPoint_reformed], weather: Weather): Array[RecommededPlaces] = {
    var recommend = new ArrayBuffer[RecommededPlaces]()
    var extreme_hotActivityTags: List[String] = List("viewpoint", "theme_park", "picnic_park", "zoo", "marketplace",
      "canoe", "cliff_diving", "scuba_diving", "sailing", "safety_training", "surfing", "water_ski", "swimming")

    var normal_hotActivityTags: List[String] = List("safety_training", "cinema", "gym",
      "restaurant", "arts_centre", "planetarium", "theatre", "picnic_park", "zoo", "marketplace", "canoe")

    val avg_temp = (weather.Temp_max + weather.Temp_min) / 2.0
    if (weather.weather_type.equals("fair") || weather.weather_type.equals("dry")) {
      if (avg_temp <= 1) {
        //Snow means Skiing is recommended
        recommend += RecommededPlaces(places.filter(point => point.sport.get.equals("skiing")), weather)
      }
      if (avg_temp >= 10 && avg_temp <= 20) {
        recommend += RecommededPlaces(places.filter(interestPoint => normal_hotActivityTags.contains(interestPoint.cultural.get)
          || normal_hotActivityTags.contains(interestPoint.leisure.get) || normal_hotActivityTags.contains(interestPoint.sport.get)
          || normal_hotActivityTags.contains(interestPoint.parks.get) || normal_hotActivityTags.contains(interestPoint.tourism.get)), weather)
      }

      if (avg_temp > 20 && avg_temp <= 40) {
        recommend += RecommededPlaces(places.filter(interestPoint => extreme_hotActivityTags.contains(interestPoint.cultural.get)
          || extreme_hotActivityTags.contains(interestPoint.leisure.get) || extreme_hotActivityTags.contains(interestPoint.sport.get)
          || extreme_hotActivityTags.contains(interestPoint.parks.get) || extreme_hotActivityTags.contains(interestPoint.tourism.get)), weather)
      }
    }
    else {
      recommend += RecommededPlaces(places.filter(interestPoint => interestPoint.cultural.getOrElse("None").equals("arts_centre")
        || interestPoint.cultural.getOrElse("None").equals("planetarium") || interestPoint.leisure.getOrElse("None").equals("cinema")
        || interestPoint.leisure.getOrElse("None").equals("gym") || interestPoint.sport.getOrElse("None").equals("9pin")
        || interestPoint.sport.getOrElse("None").equals("10pin") || interestPoint.sport.getOrElse("None").equals("boxing")
        || interestPoint.nightlife.getOrElse("None").equals("nightclub")), weather)
    }
    recommend.toArray
  }
}
