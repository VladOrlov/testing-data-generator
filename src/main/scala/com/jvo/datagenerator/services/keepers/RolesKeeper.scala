package com.jvo.datagenerator.services.keepers

import com.jvo.datagenerator.dto.entitydata.EntityMetadata
import com.jvo.datagenerator.utils.DataGeneratorUtils
import org.apache.avro.generic.GenericData

object RolesKeeper {

  val PersonRole = "PERSON"
  val CompanyRole = "COMPANY"
  val ProductRole = "PRODUCT"
  val PurchaseRole = "PURCHASE"
  val GenericRole = "GENERIC"

  val PersonFirstNameRole = "PERSON_FIRST_NAME"
  val PersonLastNameRole = "PERSON_LAST_NAME"
  val PersonMiddleNameRole = "PERSON_MIDDLE_NAME"
  val PersonFullNameRole = "PERSON_FULL_NAME"
  val PersonAgeRole = "PERSON_AGE"
  val PersonDateOfBirthRole = "PERSON_DATE_OF_BIRTH"
  val PersonPhoneRole = "PERSON_PHONE"
  val PersonEmailRole = "PERSON_EMAIL"
  val PersonAddressRole = "PERSON_ADDRESS"

  val ProductNameRole = "PRODUCT_NAME"
  val ProductPriceRole = "PRODUCT_PRICE"
  val ProductCategoryRole = "PRODUCT_CATEGORY"
  val ProductDescriptionRole = "PRODUCT_DESCRIPTION"

  val PurchaseQuantityRole = "PURCHASE_QUANTITY"
  val PurchasePriceRole = "PURCHASE_PRICE"
  val PurchaseSumRole = "PURCHASE_SUM"
  val PurchaseDiscountRole = "PURCHASE_DISCOUNT"

  val CompanyNameRole = "COMPANY_NAME"
  val CompanyPhoneRole = "COMPANY_PHONE"
  val CompanyWebsiteRole = "COMPANY_WEBSITE"
  val CompanyEmailRole = "COMPANY_EMAIL"
  val CompanyAddressRole = "COMPANY_ADDRESS"

  private[this] val entityRolesMapping: Map[String, EntityMetadata => GenericData.Record] =
    Map(
      PersonRole -> DataGeneratorUtils.mapToPersonRecord,
      CompanyRole -> DataGeneratorUtils.mapToCompanyRecord,
      ProductRole -> DataGeneratorUtils.mapToProductRecord,
      PurchaseRole -> DataGeneratorUtils.mapToPurchaseRecord,
      GenericRole -> DataGeneratorUtils.mapToGenericRecord
    )

  val fieldRoles: Map[String, Set[String]] = Map(
    PersonRole -> Set(PersonFirstNameRole, PersonLastNameRole, PersonMiddleNameRole, PersonFullNameRole, PersonPhoneRole,
      PersonEmailRole, PersonAddressRole, PersonAgeRole, PersonDateOfBirthRole),
    CompanyRole -> Set(CompanyNameRole, CompanyAddressRole, CompanyPhoneRole, CompanyWebsiteRole, CompanyEmailRole),
    ProductRole -> Set(ProductNameRole, ProductPriceRole),
    PurchaseRole -> Set(PurchaseQuantityRole, PurchaseSumRole, PurchasePriceRole))

  def getMapToRecordFunction(role: String): EntityMetadata => GenericData.Record = {
    entityRolesMapping.getOrElse(role, DataGeneratorUtils.mapToGenericRecord)
  }

  def getMapToRecordFunction(maybeRole: Option[String]): EntityMetadata => GenericData.Record = {
    maybeRole
      .flatMap(role => entityRolesMapping.get(role))
      .getOrElse(DataGeneratorUtils.mapToGenericRecord)
  }

}
